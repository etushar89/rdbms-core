/*
 * buffer_mgr.c
 *
 *  Created on: Sep 27, 2015
 *      Author: heisenberg
 */

#include "buffer_mgr.h"

#include <stdio.h>
#include <stdlib.h>

size_t strlen(const char *);
char *strcpy(char *, const char *);

/**
 * Initializes buffer pool.
 *
 * bm = buffer pool handle
 * pageFileName = name of the underlying page file for which this pool is being created
 * numPages = no of pages this pool can hold in memory at a time
 * strategy = page replacement strategy used to swap out pages when needed
 * stratData = additional replacement strategy configuration parameters
 */
RC initBufferPool(BM_BufferPool * const bm, const char * const pageFileName,
		const int numPages, ReplacementStrategy strategy, void *stratData) {

	//Sanity checks
	if (bm == NULL) {
		THROW(RC_INVALID_HANDLE, "Buffer pool handle is invalid");
	}
	if (pageFileName == NULL) {
		THROW(RC_INVALID_PAGE_FILE_NAME, "Page file name is invalid");
	}
	if (numPages < 0) {
		THROW(RC_INVALID_PAGE_NUM, "Invalid numPages");
	}

	//Ideally we shouldn't need mutex on init, shutdown functions,
	//because clients are expected to use these functions properly
	//i.e. there shouldn't be multiple threads calling these functions simultaneously
	//One client should use these functions only once at a time.

	//But we'll assume clients are bad and use locks on these functions also.

	//Init Global lock
	pthread_mutex_init(&GLOBAL_LOCK, NULL);
	//Init locks specific to page frame access
	pthread_mutex_init(&PAGE_FRAME_LOCK, NULL);
	//Acquire lock
	pthread_mutex_lock(&GLOBAL_LOCK);
	//Acquire page frames access lock also
	pthread_mutex_lock(&PAGE_FRAME_LOCK);

	//Set capacity of pool
	bm->numPages = numPages;

	//Set page file name in pool
	bm->pageFile = (char*) malloc(strlen(pageFileName) + sizeof(char));
	strcpy(bm->pageFile, pageFileName);

	bm->strategy = strategy;

	//Set additional metadata for the pool
	bm->mgmtData = (BM_Data *) malloc(sizeof(BM_Data));

	//pages is the actual array holding page frames
	((BM_Data *) bm->mgmtData)->pages = (BM_PageHandle **) malloc(
			sizeof(BM_PageHandle *) * numPages);

	//dirtyFlags array hold dirty-ness status of pages
	((BM_Data *) bm->mgmtData)->dirtyFlags = (bool *) malloc(
			numPages * sizeof(bool));

	//fixCount array holds fix count of pages
	((BM_Data *) bm->mgmtData)->fixCount = (PageNumber *) malloc(
			numPages * sizeof(PageNumber));

	//pageInTime array holds epoch time when page was brought in pool
	((BM_Data *) bm->mgmtData)->pageInTime = (struct timeval *) malloc(
			numPages * sizeof(struct timeval));

	//pageUsedTime array holds epoch time when page was last used
	((BM_Data *) bm->mgmtData)->pageUsedTime = (struct timeval *) malloc(
			numPages * sizeof(struct timeval));

	//pageUsedTime array holds epoch time when page was last used
	((BM_Data *) bm->mgmtData)->pageUsedCount = (int *) malloc(
			numPages * sizeof(int));

	//frameIndexMap holds page no and it's index in pool
	((BM_Data *) bm->mgmtData)->pageFrameIndexMap = (PageNumber *) malloc(
			numPages * sizeof(PageNumber));

	int i = 0;
	for (; i < numPages; i++) {
		((BM_Data *) bm->mgmtData)->pageFrameIndexMap[i] = NO_PAGE;
		((BM_Data *) bm->mgmtData)->fixCount[i] = 0;
		((BM_Data *) bm->mgmtData)->pages[i] = NULL;
		((BM_Data *) bm->mgmtData)->dirtyFlags[i] = FALSE;
		((BM_Data *) bm->mgmtData)->pageInTime[i].tv_usec = -1;
		((BM_Data *) bm->mgmtData)->pageUsedTime[i].tv_usec = -1;
		((BM_Data *) bm->mgmtData)->pageUsedCount[i] = 0;
	}

	((BM_Data *) bm->mgmtData)->numDirtyPages = 0;
	((BM_Data *) bm->mgmtData)->numPinnedPages = 0;
	((BM_Data *) bm->mgmtData)->numReadIO = 0;
	((BM_Data *) bm->mgmtData)->numWriteIO = 0;
	((BM_Data *) bm->mgmtData)->newBlockRequested = FALSE;
	((BM_Data *) bm->mgmtData)->extraBlockReqCount = 0;
	((BM_Data *) bm->mgmtData)->pageHit = 0;
	((BM_Data *) bm->mgmtData)->pinReqCount = 0;

	//Open underlying page file
	openPageFile(bm->pageFile, &(((BM_Data *) bm->mgmtData)->smFH));

	((BM_Data *) bm->mgmtData)->actualPageFileCnt =
			((BM_Data *) bm->mgmtData)->smFH.totalNumPages;

	//Release page frames access lock
	pthread_mutex_unlock(&PAGE_FRAME_LOCK);
	//Release lock
	pthread_mutex_unlock(&GLOBAL_LOCK);

	//All OK
	return RC_OK;
}

/**
 * Force writes any dirty pages to disk and if all pages have fix count of 0,
 * buffer manager resources are released
 *
 * bm = buffer pool handle
 */
RC shutdownBufferPool(BM_BufferPool * const bm) {

	//Sanity checks
	if (bm == NULL) {
		THROW(RC_INVALID_HANDLE, "Buffer pool handle is invalid");
	}

	//Don't allow shutdown if there are pinned pages
	if (((BM_Data *) bm->mgmtData)->numPinnedPages != 0) {
		THROW(RC_SHUTDOWN_FAIL,
				"There are some pages pinned in memory, cannot shutdown now");
	}

	//Acquire lock
	pthread_mutex_lock(&GLOBAL_LOCK);
	//Acquire page frames access lock
	pthread_mutex_lock(&PAGE_FRAME_LOCK);

	int i;
	writeNewBlocks(bm, -1);

	//Write all dirty pages to disk.
	if (((BM_Data *) bm->mgmtData)->numDirtyPages > 0) {
		for (i = 0; i < bm->numPages; i++) {
			if (((BM_Data *) bm->mgmtData)->pageFrameIndexMap[i] != NO_PAGE
					&& ((BM_Data *) bm->mgmtData)->dirtyFlags[i] == TRUE) {
				//Ensure enough blocks exist in underlying pagefile
				writeBlock(((BM_Data *) bm->mgmtData)->pageFrameIndexMap[i],
						&(((BM_Data *) bm->mgmtData)->smFH),
						((BM_Data *) bm->mgmtData)->pages[i]->data);
				//Reset dirty flag
				((BM_Data *) bm->mgmtData)->dirtyFlags[i] = FALSE;
				//Update IO Count
				((BM_Data *) bm->mgmtData)->numWriteIO++;
				//Decrement dirty page count
				((BM_Data *) bm->mgmtData)->numDirtyPages--;
			}
		}
	}

#ifdef _DEBUG
	printf("\n Arrays Before Shutdown:");
	printDebugInfo(bm);
#endif

	//Close underlying page file
	closePageFile(&(((BM_Data *) bm->mgmtData)->smFH));

#ifdef _DEBUG
	printf("\n Stats before shutdown: ");
	printIOStat(bm);
#endif

	BM_PageHandle **pages = ((BM_Data *) bm->mgmtData)->pages;
	//Release memory allocated for internal page frames
	for (i = 0; i < bm->numPages; i++) {
		if (pages[i] != NULL) {
			if (pages[i]->data != NULL) {
				free(((BM_Data*) bm->mgmtData)->pages[i]->data);
			}
			free(((BM_Data*) bm->mgmtData)->pages[i]);
		}
	}

	free(((BM_Data *) bm->mgmtData)->fixCount);
	((BM_Data *) bm->mgmtData)->fixCount = NULL;
	free(((BM_Data *) bm->mgmtData)->dirtyFlags);
	((BM_Data *) bm->mgmtData)->dirtyFlags = NULL;
	free(((BM_Data *) bm->mgmtData)->pageFrameIndexMap);
	((BM_Data *) bm->mgmtData)->pageFrameIndexMap = NULL;
	free(((BM_Data *) bm->mgmtData)->pageInTime);
	((BM_Data *) bm->mgmtData)->pageInTime = NULL;
	free(((BM_Data *) bm->mgmtData)->pageUsedTime);
	((BM_Data *) bm->mgmtData)->pageUsedTime = NULL;
	free(((BM_Data *) bm->mgmtData)->pageUsedCount);
	((BM_Data *) bm->mgmtData)->pageUsedCount = NULL;
	free(((BM_Data *) bm->mgmtData)->pages);
	((BM_Data *) bm->mgmtData)->pages = NULL;
	free(bm->mgmtData);
	bm->mgmtData = NULL;
	free(bm->pageFile);
	bm->pageFile = NULL;

	//Release page frames access lock
	pthread_mutex_unlock(&PAGE_FRAME_LOCK);
	//Release lock
	pthread_mutex_unlock(&GLOBAL_LOCK);
	//Destroy the page frames lock
	pthread_mutex_destroy(&PAGE_FRAME_LOCK);
	//Destroy the global lock
	pthread_mutex_destroy(&GLOBAL_LOCK);

	return RC_OK;
}

/**
 * Writes all dirty pages from buffer pool with fix count of 0 to disk
 *
 * bm = buffer pool handle
 */
RC forceFlushPool(BM_BufferPool * const bm) {

	//Sanity checks
	if (bm == NULL) {
		THROW(RC_INVALID_HANDLE, "Buffer pool handle is invalid");
	}

	//Acquire lock
	pthread_mutex_lock(&GLOBAL_LOCK);
	//Acquire page frames access lock
	pthread_mutex_lock(&PAGE_FRAME_LOCK);

	if (((BM_Data *) bm->mgmtData)->numDirtyPages > 0) {
		writeNewBlocks(bm, -1);
		//Write all dirty pages with fix count 0 to disk.
		int i;
		for (i = 0; i < bm->numPages; i++) {
			if (((BM_Data *) bm->mgmtData)->pageFrameIndexMap[i] != NO_PAGE
					&& ((BM_Data *) bm->mgmtData)->dirtyFlags[i] == TRUE
					&& ((BM_Data *) bm->mgmtData)->fixCount[i] == 0) {
				//Ensure enough blocks exist in underlying pagefile
				writeBlock(((BM_Data *) bm->mgmtData)->pageFrameIndexMap[i],
						&(((BM_Data *) bm->mgmtData)->smFH),
						((BM_Data *) bm->mgmtData)->pages[i]->data);
				//Reset dirty flag
				((BM_Data *) bm->mgmtData)->dirtyFlags[i] = FALSE;
				//Update IO Count
				((BM_Data *) bm->mgmtData)->numWriteIO++;
				//Decrement dirty page count
				((BM_Data *) bm->mgmtData)->numDirtyPages--;
			}
		}
	}

	//Release page frames access lock
	pthread_mutex_unlock(&PAGE_FRAME_LOCK);
	//Release lock
	pthread_mutex_unlock(&GLOBAL_LOCK);

	//All OK
	return RC_OK;
}
