#include "buffer_mgr_stat.h"
#include "buffer_mgr.h"

#include <stdio.h>
#include <stdlib.h>

// local functions
static void printStrat(BM_BufferPool * const bm);

/*
 * Returns an array of PageNumbers (of size numPages) where the ith
 * element is the number of the page stored in the ith page frame
 *
 * bm = buffer pool handle
 */
PageNumber *getFrameContents(BM_BufferPool * const bm) {
	return ((BM_Data *) bm->mgmtData)->pageFrameIndexMap;
}

/*
 * Returns an array of bools (of size numPages) where the ith
 * element is TRUE if the page stored in the ith page frame is dirty.
 * Empty page frames are considered as clean.
 *
 * bm = buffer pool handle
 */
bool *getDirtyFlags(BM_BufferPool * const bm) {
	return ((BM_Data *) bm->mgmtData)->dirtyFlags;
}

/*
 * Returns an array of ints (of size numPages) where the ith
 * element is the fix count of the page stored in the ith page frame.
 * Returns 0 for empty page frames.
 *
 * bm = buffer pool handle
 */
int *getFixCounts(BM_BufferPool * const bm) {
	return ((BM_Data *) bm->mgmtData)->fixCount;
}

/*
 * Returns the number of pages that have been read from disk
 * since a buffer pool has been initialized
 *
 * bm = buffer pool handle
 */
int getNumReadIO(BM_BufferPool * const bm) {

	//Sanity checks
	if (bm == NULL) {
		THROW(RC_INVALID_HANDLE, "Buffer pool handle is invalid");
	}

	return ((BM_Data *) bm->mgmtData)->numReadIO;
}

/*
 * Returns the number of pages that have been written to disk
 * since a buffer pool has been initialized
 *
 * bm = buffer pool handle
 */
int getNumWriteIO(BM_BufferPool * const bm) {

	//Sanity checks
	if (bm == NULL) {
		THROW(RC_INVALID_HANDLE, "Buffer pool handle is invalid");
	}

	return ((BM_Data *) bm->mgmtData)->numWriteIO;
}

float getPageHitCount(BM_BufferPool * const bm) {
	return ((BM_Data *) bm->mgmtData)->pageHit;
}

float getPageHitRatio(BM_BufferPool * const bm) {
	((BM_Data *) bm->mgmtData)->hitRatio = ((BM_Data *) bm->mgmtData)->pageHit
			/ ((BM_Data *) bm->mgmtData)->pinReqCount;
	return ((BM_Data *) bm->mgmtData)->hitRatio;
}

// external functions
void printPoolContent(BM_BufferPool * const bm) {
	PageNumber *frameContent;
	bool *dirty;
	int *fixCount;
	int i;

	frameContent = getFrameContents(bm);
	dirty = getDirtyFlags(bm);
	fixCount = getFixCounts(bm);

	printf("{");
	printStrat(bm);
	printf(" %i}: ", bm->numPages);

	for (i = 0; i < bm->numPages; i++)
		printf("%s[%i%s%i]", ((i == 0) ? "" : ","), frameContent[i],
				(dirty[i] ? "x" : " "), fixCount[i]);
	printf("\n");
}

char *
sprintPoolContent(BM_BufferPool * const bm) {
	PageNumber *frameContent;
	bool *dirty;
	int *fixCount;
	int i;
	char *message;
	int pos = 0;

	message = (char *) malloc(256 + (22 * bm->numPages));
	frameContent = getFrameContents(bm);
	dirty = getDirtyFlags(bm);
	fixCount = getFixCounts(bm);

	for (i = 0; i < bm->numPages; i++)
		pos += sprintf(message + pos, "%s[%i%s%i]", ((i == 0) ? "" : ","),
				frameContent[i], (dirty[i] ? "x" : " "), fixCount[i]);

	return message;
}

void printPageContent(BM_PageHandle * const page) {
	int i;

	printf("[Page %i]\n", page->pageNum);

	for (i = 1; i <= PAGE_SIZE; i++)
		printf("%02X%s%s", page->data[i], (i % 8) ? "" : " ",
				(i % 64) ? "" : "\n");
}

char *
sprintPageContent(BM_PageHandle * const page) {
	int i;
	char *message;
	int pos = 0;

	message = (char *) malloc(
			30 + (2 * PAGE_SIZE) + (PAGE_SIZE % 64) + (PAGE_SIZE % 8));
	pos += sprintf(message + pos, "[Page %i]\n", page->pageNum);

	for (i = 1; i <= PAGE_SIZE; i++)
		pos += sprintf(message + pos, "%02X%s%s", page->data[i],
				(i % 8) ? "" : " ", (i % 64) ? "" : "\n");

	return message;
}

void printIOStat(BM_BufferPool * const bm) {
	printf("\n## Read IO: %d", getNumReadIO(bm));
	printf("\n## Write IO: %d\n", getNumWriteIO(bm));
	printf("\n## Page Hits: %f\n", getPageHitCount(bm));
	printf("\n## Page Hit Ratio: %f\n", getPageHitRatio(bm));
}

void printStrat(BM_BufferPool * const bm) {
	switch (bm->strategy) {
	case RS_FIFO:
		printf("FIFO");
		break;
	case RS_LRU:
		printf("LRU");
		break;
	case RS_CLOCK:
		printf("CLOCK");
		break;
	case RS_LFU:
		printf("LFU");
		break;
	case RS_LRU_K:
		printf("LRU-K");
		break;
	default:
		printf("%i", bm->strategy);
		break;
	}
}
