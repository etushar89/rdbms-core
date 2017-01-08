/*
 * buffer_mgr.c
 *
 *  Created on: Sep 27, 2015
 *      Author: heisenberg
 */

#include "buffer_mgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>

#define PRIVATE static

void *memset(void *, int, size_t);

PRIVATE inline int getPageFrameIndex(BM_BufferPool * const, const PageNumber);
PRIVATE inline int getFreeFrameIndex(BM_BufferPool * const);
PRIVATE inline void checkAndSwapPage(BM_BufferPool * const, PageNumber);

/**
 * Marks a page in buffer pool as modified / dirtied
 *
 * bm = buffer pool handle
 * page = page handle to hold data and corresponding page number
 */
RC markDirty(BM_BufferPool * const bm, BM_PageHandle * const page) {

	//Sanity checks
	if (bm == NULL) {
		THROW(RC_INVALID_HANDLE, "Buffer pool handle is invalid");
	}
	if (page == NULL) {
		THROW(RC_INVALID_HANDLE, "Page handle is invalid");
	}

	//Look up if requested page already exists in pool
	int index = getPageFrameIndex(bm, page->pageNum);

	if (index == -1) {
		THROW(RC_PAGE_NOT_PINNED, "Requested page has not been pinned");
	}

	//Acquire GLOBAL lock
	pthread_mutex_lock(&GLOBAL_LOCK);
	//Check if page is already marked as dirty
	if (((BM_Data *) bm->mgmtData)->dirtyFlags[index] == FALSE) {
		//Mark page as dirty
		((BM_Data *) bm->mgmtData)->dirtyFlags[index] = TRUE;
		//Increment dirty page count
		((BM_Data *) bm->mgmtData)->numDirtyPages++;
	}
	//Release lock
	pthread_mutex_unlock(&GLOBAL_LOCK);

	//All OK
	return RC_OK;
}

/**
 * Remove the page from buffer pool.
 * However, we are using Lazy Page Swap Out strategy:
 * Page is only marked for swap out from pool, but it's not actually swapped out
 * until pool gets full and a vacant frame is needed. This approach avoid disk reads
 * if other client requests same page before pool gets full.
 *
 * bm = buffer pool handle
 * page = page handle to hold data and corresponding page number
 */
RC unpinPage(BM_BufferPool * const bm, BM_PageHandle * const page) {

	//Sanity checks
	if (bm == NULL) {
		THROW(RC_INVALID_HANDLE, "Buffer pool handle is invalid");
	}
	if (page == NULL) {
		THROW(RC_INVALID_HANDLE, "Page handle is invalid");
	}

	//Look up if requested page already exists in pool
	int index = getPageFrameIndex(bm, page->pageNum);

	//Index = -1 indicates page isn't available in pool
	if (index == -1) {
		THROW(RC_PAGE_NOT_EXIST, "Requested page doesn't exist in buffer pool");
	}

	//Acquire GLOBAL lock
	pthread_mutex_lock(&GLOBAL_LOCK);
	if (((BM_Data *) bm->mgmtData)->fixCount[index] == 0) {
		//Release lock
		pthread_mutex_unlock(&GLOBAL_LOCK);
		THROW(RC_PAGE_NOT_PINNED, "Requested page has not been pinned");
	}
	//Update fix count of pinned page
	((BM_Data *) bm->mgmtData)->fixCount[index]--;
	//Decrement pin count
	((BM_Data *) bm->mgmtData)->numPinnedPages--;
	//Release lock
	pthread_mutex_unlock(&GLOBAL_LOCK);

	//All OK
	return RC_OK;
}

/**
 * Force write a page to disk irrespective of whether it's dirty or not
 *
 * bm = buffer pool handle
 * page = page handle to hold data and corresponding page number
 */
RC forcePage(BM_BufferPool * const bm, BM_PageHandle * const page) {

	//Sanity checks
	if (bm == NULL) {
		THROW(RC_INVALID_HANDLE, "Buffer pool handle is invalid");
	}
	if (page == NULL) {
		THROW(RC_INVALID_HANDLE, "Page handle is invalid");
	}

	//Look up if requested page already exists in pool
	int index = getPageFrameIndex(bm, page->pageNum);

	//Index = -1 indicates page isn't available in pool
	if (index == -1) {
		THROW(RC_PAGE_NOT_EXIST, "Requested page doesn't exist in buffer pool");
	}

	//Acquire GLOBAL lock
	pthread_mutex_lock(&GLOBAL_LOCK);
	//Ensure enough blocks exist in underlying pagefile
	writeNewBlocks(bm, -1);
	writeBlock(page->pageNum, &(((BM_Data *) bm->mgmtData)->smFH), page->data);

	if (((BM_Data *) bm->mgmtData)->dirtyFlags[index] == TRUE) {
		//Decrement dirty page count
		((BM_Data *) bm->mgmtData)->numDirtyPages--;
		//Reset dirty flag
		((BM_Data *) bm->mgmtData)->dirtyFlags[index] = FALSE;
	}

	//Update IO Count
	((BM_Data *) bm->mgmtData)->numWriteIO++;
	//Release lock
	pthread_mutex_unlock(&GLOBAL_LOCK);

	//All OK
	return RC_OK;
}

/**
 * Pins page to an empty page frame if it doesn't already exist in any of the frames.
 * Otherwise data from already pinned page is returned.
 * Pinning takes advantage of Lazy Page Swap Out strategy: Page marked for swap out is
 * actually swapped out of memory only when all the frames are full and new page is needed.
 * Otherwise, only the frame is marked as empty. This saves disk read delay in case another
 * client requests same page.
 *
 * bm = buffer pool handle
 * page = page handle to hold data and corresponding page number
 * pageNum = index of the page to be pinned
 */
RC pinPage(BM_BufferPool * const bm, BM_PageHandle * const page,
		const PageNumber pageNum) {

	//Sanity checks
	if (bm == NULL) {
		THROW(RC_INVALID_HANDLE, "Buffer pool handle is invalid");
	}
	if (page == NULL) {
		THROW(RC_INVALID_HANDLE, "Page handle is invalid");
	}
	if (pageNum < 0) {
		THROW(RC_INVALID_PAGE_REQUESTED, "Invalid page requested for pin");
	}

	//Acquire GLOBAL lock, as Pin functionality modifies almost all shared data
	pthread_mutex_lock(&GLOBAL_LOCK);

	//Look up if requested page already exists in pool
	int index = getPageFrameIndex(bm, pageNum);

	//Index = -1 indicates page isn't available in pool
	if (index == -1) {
		//Now we need to fetch the requested page from disk and pin it in pool

		//Check if empty page frame is available to accommodate new page
		if (((BM_Data *) bm->mgmtData)->numPinnedPages == bm->numPages) {
			//Release lock
			pthread_mutex_unlock(&GLOBAL_LOCK);
			THROW(RC_ALL_FRAMES_OCCUPIED,
					"All frames are occupied by pinned pages");
		}

		index = getFreeFrameIndex(bm);

		if (index == -1) {
			//Release lock
			pthread_mutex_unlock(&GLOBAL_LOCK);
			THROW(RC_ALL_FRAMES_OCCUPIED,
					"Couldn't get empty page frame for page");
		}

		//Acquire page frames access lock
		pthread_mutex_lock(&PAGE_FRAME_LOCK);
		//Remove previous page from frame, if present
		if (((BM_Data *) bm->mgmtData)->pages[index] != NULL) {
			if (((BM_Data *) bm->mgmtData)->pages[index]->data != NULL) {
				free(((BM_Data*) bm->mgmtData)->pages[index]->data);
			}
			free(((BM_Data*) bm->mgmtData)->pages[index]);
		}

		//Allocate memory to page frame to hold incoming page data
		((BM_Data *) bm->mgmtData)->pages[index] = (BM_PageHandle *) malloc(
				sizeof(BM_PageHandle));
		((BM_Data *) bm->mgmtData)->pages[index]->data = (SM_PageHandle) malloc(
				PAGE_SIZE);

		//Read requested page from page file on disk
		RC ret = readBlock(pageNum, &(((BM_Data *) bm->mgmtData)->smFH),
				((BM_Data *) bm->mgmtData)->pages[index]->data);

		//Release page frames access lock
		pthread_mutex_unlock(&PAGE_FRAME_LOCK);

		if (ret == RC_OK) {
			((BM_Data *) bm->mgmtData)->numReadIO++;
		}
		//Check if requested page was available in page file on disk
		else if (ret == RC_READ_NON_EXISTING_PAGE) {
			((BM_Data *) bm->mgmtData)->newBlockRequested = TRUE;
			((BM_Data *) bm->mgmtData)->dirtyFlags[index] = TRUE;
			((BM_Data *) bm->mgmtData)->numDirtyPages++;
			//Acquire page frames access lock
			pthread_mutex_lock(&PAGE_FRAME_LOCK);
			//Now that the block is new, it must contain all NULLs, don't read from disk, it's slow
			memset(((BM_Data *) bm->mgmtData)->pages[index]->data, '\0',
					PAGE_SIZE);
			//Release page frames access lock
			pthread_mutex_unlock(&PAGE_FRAME_LOCK);
			((BM_Data *) bm->mgmtData)->extraBlockReqCount++;
		}
		//Some other error occurred
		else {
			//Release lock
			pthread_mutex_unlock(&GLOBAL_LOCK);
			THROW(ret, "Page read from page file failed");
		}

		//Acquire page frames access lock
		pthread_mutex_lock(&PAGE_FRAME_LOCK);
		//Set page number in frame
		((BM_Data *) bm->mgmtData)->pages[index]->pageNum = pageNum;
		//Release page frames access lock
		pthread_mutex_unlock(&PAGE_FRAME_LOCK);

		gettimeofday(&(((BM_Data *) bm->mgmtData)->pageInTime[index]), NULL);
	} else {
		//Page Hit
		((BM_Data *) bm->mgmtData)->pageHit++;
	}

	//Increment pin request counter
	((BM_Data *) bm->mgmtData)->pinReqCount++;
	//Page already exists in pool, simply point page handle to existing data
	page->pageNum = pageNum;
	//Acquire page frames access lock
	pthread_mutex_lock(&PAGE_FRAME_LOCK);
	//Point page data to page in frame's data
	page->data = ((BM_Data *) bm->mgmtData)->pages[index]->data;
	//Release page frames access lock
	pthread_mutex_unlock(&PAGE_FRAME_LOCK);
	//Update page and frame index mapping
	((BM_Data *) bm->mgmtData)->pageFrameIndexMap[index] = pageNum;
	//Update fix count of pinned page
	((BM_Data *) bm->mgmtData)->fixCount[index]++;
	//Update use time stamp
	gettimeofday(&(((BM_Data *) bm->mgmtData)->pageUsedTime[index]), NULL);
	//Increment page usage count
	((BM_Data *) bm->mgmtData)->pageUsedCount[index]++;
	//Increment pin count
	((BM_Data *) bm->mgmtData)->numPinnedPages++;

#ifdef _DEBUG
	printf("\n Pinned Page: %d", pageNum);
	printDebugInfo(bm);
#endif

	//Release lock
	pthread_mutex_unlock(&GLOBAL_LOCK);

	//All OK
	return RC_OK;
}

/**
 * Private utility function to ensure that non-existing pages pinned get their
 * corresponding blocks written to pagefile before actual page
 *
 * bm = buffer pool handle
 * num = page index to check if we find new page being written of that index
 */
bool inline writeNewBlocks(BM_BufferPool * const bm, PageNumber num) {

	bool ret = FALSE;
	if (((BM_Data *) bm->mgmtData)->newBlockRequested == TRUE) {
		int i;
		for (i = 0; i < ((BM_Data *) bm->mgmtData)->extraBlockReqCount; i++) {
			int cBlock = ((BM_Data *) bm->mgmtData)->actualPageFileCnt + i;
			//Look up if requested page already exists in pool
			int index = getPageFrameIndex(bm, cBlock);

			if ((index != -1)
					&& ((BM_Data *) bm->mgmtData)->dirtyFlags[index] == TRUE) {
				//Acquire page frames access lock
				pthread_mutex_lock(&PAGE_FRAME_LOCK);
				appendEmptyBlockData(&(((BM_Data *) bm->mgmtData)->smFH),
						((BM_Data *) bm->mgmtData)->pages[index]->data);
				//Release page frames access lock
				pthread_mutex_unlock(&PAGE_FRAME_LOCK);
				ret = index == num;
				//Decrement dirty page count
				((BM_Data *) bm->mgmtData)->numDirtyPages--;
				//Reset dirty flag
				((BM_Data *) bm->mgmtData)->dirtyFlags[index] = FALSE;
				//Update IO Count
				((BM_Data *) bm->mgmtData)->numWriteIO++;
			} else {
				appendEmptyBlockData(&(((BM_Data *) bm->mgmtData)->smFH), NULL);
			}
		}
		((BM_Data *) bm->mgmtData)->newBlockRequested = FALSE;
		((BM_Data *) bm->mgmtData)->extraBlockReqCount = 0;
		((BM_Data *) bm->mgmtData)->actualPageFileCnt =
				((BM_Data *) bm->mgmtData)->smFH.totalNumPages;
	}

	return ret;
}

/**
 * Private utility function to find index of page frame of a specific
 * page with page number pageNum
 *
 * bm = buffer pool handle
 * pageNum = page number to be looked up
 */
PRIVATE inline int getPageFrameIndex(BM_BufferPool * const bm,
		const PageNumber pageNum) {

	int i, index = -1;

	//Acquire page frames access lock
	pthread_mutex_lock(&PAGE_FRAME_LOCK);
	BM_PageHandle **pages = ((BM_Data *) bm->mgmtData)->pages;
	//Scan through each page to get index of requested page
	for (i = 0; i < bm->numPages; i++) {
		if ((pages[i] != NULL) && pages[i]->pageNum == pageNum) {
			index = i;
			//Release page frames access lock
			pthread_mutex_unlock(&PAGE_FRAME_LOCK);
			break;
		}
	}
	//Release page frames access lock
	pthread_mutex_unlock(&PAGE_FRAME_LOCK);

	return index;
}

/**
 *	Private utility function to find free frame index within
 *	internal page - frame mapping array
 *
 *	bm = buffer pool handle
 */
PRIVATE inline int getFreeFrameIndex(BM_BufferPool * const bm) {

	int i, freeIndex = -1, lfuIndex = -1, lruIndex = -1, firstInIndex = -1;

	//Look for free page frame
	for (i = 0; i < bm->numPages; i++) {
		if (((BM_Data *) bm->mgmtData)->pageFrameIndexMap[i] == NO_PAGE) {
			//Free page frame found with index i
			freeIndex = i;
			break;
		}
		if (((BM_Data *) bm->mgmtData)->fixCount[i] == 0) {
			if (firstInIndex == -1) {
				firstInIndex = i;
				lruIndex = i;
				lfuIndex = i;
			} else {
				if (((BM_Data *) bm->mgmtData)->pageInTime[i].tv_usec
						< ((BM_Data *) bm->mgmtData)->pageInTime[firstInIndex].tv_usec)
					firstInIndex = i;
				if (((BM_Data *) bm->mgmtData)->pageUsedTime[i].tv_usec
						< ((BM_Data *) bm->mgmtData)->pageUsedTime[lruIndex].tv_usec)
					lruIndex = i;
				if (((BM_Data *) bm->mgmtData)->pageUsedCount[i]
															  < ((BM_Data *) bm->mgmtData)->pageUsedCount[lfuIndex])
					lfuIndex = i;
				if (((BM_Data *) bm->mgmtData)->pageUsedCount[i]
															  == ((BM_Data *) bm->mgmtData)->pageUsedCount[lfuIndex])
					if (((BM_Data *) bm->mgmtData)->pageInTime[i].tv_usec
							< ((BM_Data *) bm->mgmtData)->pageInTime[lfuIndex].tv_usec)
						lfuIndex = i;
			}
		}
	}

	//If free page frame is not found
	if (freeIndex == -1) {
		if (bm->strategy == RS_FIFO) {
			freeIndex = firstInIndex;
		} else if (bm->strategy == RS_LRU) {
			freeIndex = lruIndex;
		} else if (bm->strategy == RS_LFU) {
			freeIndex = lfuIndex;
		}
		if (freeIndex != -1)
			checkAndSwapPage(bm, freeIndex);
	}

	return freeIndex;
}

PRIVATE inline void checkAndSwapPage(BM_BufferPool * const bm, PageNumber num) {
	if (((BM_Data *) bm->mgmtData)->dirtyFlags[num] == TRUE) {
		//Ensure enough blocks exist in underlying pagefile
		if (!writeNewBlocks(bm, num)) {
			//Acquire page frames access lock
			pthread_mutex_lock(&PAGE_FRAME_LOCK);
			writeBlock(((BM_Data *) bm->mgmtData)->pageFrameIndexMap[num],
					&(((BM_Data *) bm->mgmtData)->smFH),
					((BM_Data *) bm->mgmtData)->pages[num]->data);
			//Release page frames access lock
			pthread_mutex_unlock(&PAGE_FRAME_LOCK);
			//Decrement dirty page count
			((BM_Data *) bm->mgmtData)->numDirtyPages--;
			//Reset dirty flag
			((BM_Data *) bm->mgmtData)->dirtyFlags[num] = FALSE;
			//Update IO Count
			((BM_Data *) bm->mgmtData)->numWriteIO++;
		}

	}
	((BM_Data *) bm->mgmtData)->pageInTime[num].tv_usec = -1;
	((BM_Data *) bm->mgmtData)->pageUsedTime[num].tv_usec = -1;
	((BM_Data *) bm->mgmtData)->pageUsedCount[num] = 0;
	//Update page and frame index mapping
	((BM_Data *) bm->mgmtData)->pageFrameIndexMap[num] = NO_PAGE;
}

/**
 * Debug function to print contents of pageFrameIndexMap
 */
void inline printDebugInfo(BM_BufferPool * const bm) {
	int i;
	printf("\n\n Frame Index Map: ");
	for (i = 0; i < bm->numPages; i++) {
		printf("  {%d,%d}, ", i,
				((BM_Data *) bm->mgmtData)->pageFrameIndexMap[i]);
	}

	printf("\n Pages: ");
	BM_PageHandle **pages = ((BM_Data *) bm->mgmtData)->pages;
	for (i = 0; i < bm->numPages; i++) {
		if ((pages[i] != NULL)) {
			printf("  {%d,%d}, ", i, pages[i]->pageNum);
		}
	}

	printf("\n Fix Count Array: ");
	for (i = 0; i < bm->numPages; i++) {
		printf("  {%d,%d}, ", i, ((BM_Data *) bm->mgmtData)->fixCount[i]);
	}

	printf("\n Dirty Flags Array: ");
	for (i = 0; i < bm->numPages; i++) {
		printf("  {%d,%d}, ", i, ((BM_Data *) bm->mgmtData)->dirtyFlags[i]);
	}

	printf("\n Usage Count Array: ");
	for (i = 0; i < bm->numPages; i++) {
		printf("  {%d,%d}, ", i, ((BM_Data *) bm->mgmtData)->pageUsedCount[i]);
	}
	printf("\n");
}
