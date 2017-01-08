#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

// Include return codes and methods for logging errors
#include "dberror.h"
#include "storage_mgr.h"
#include <sys/time.h>
#include <pthread.h>

// Include bool DT
#include "dt.h"

// Replacement Strategies
typedef enum ReplacementStrategy {
	RS_FIFO = 0, RS_LRU = 1, RS_CLOCK = 2, RS_LFU = 3, RS_LRU_K = 4
} ReplacementStrategy;

// Data Types and Structures
typedef int PageNumber;
#define NO_PAGE -1

typedef struct BM_BufferPool {
	char *pageFile;
	int numPages;
	ReplacementStrategy strategy;
	void *mgmtData; // use this one to store the bookkeeping info your buffer
	// manager needs for a buffer pool
} BM_BufferPool;

typedef struct BM_PageHandle {
	PageNumber pageNum;
	char *data;
} BM_PageHandle;

typedef struct BM_Data {
	int numDirtyPages;
	int numPinnedPages;
	int numReadIO;
	int numWriteIO;
	bool newBlockRequested;
	int actualPageFileCnt;
	int extraBlockReqCount;
	struct timeval *pageInTime;
	struct timeval *pageUsedTime;
	int *pageUsedCount;
	float pageHit;
	float pinReqCount;
	float hitRatio;
	SM_FileHandle smFH;
	PageNumber *pageFrameIndexMap;
	bool *dirtyFlags;
	PageNumber *fixCount;
	BM_PageHandle **pages;
} BM_Data;

pthread_mutex_t GLOBAL_LOCK;
pthread_mutex_t PAGE_FRAME_LOCK;

// convenience macros
#define MAKE_POOL()					\
		((BM_BufferPool *) malloc (sizeof(BM_BufferPool)))

#define MAKE_PAGE_HANDLE()				\
		((BM_PageHandle *) malloc (sizeof(BM_PageHandle)))

// Buffer Manager Interface Pool Handling
extern RC initBufferPool(BM_BufferPool * const bm,
		const char * const pageFileName, const int numPages,
		ReplacementStrategy strategy, void *stratData);
extern RC shutdownBufferPool(BM_BufferPool * const bm);
extern RC forceFlushPool(BM_BufferPool * const bm);

// Buffer Manager Interface Access Pages
extern RC markDirty(BM_BufferPool * const bm, BM_PageHandle * const page);
extern RC unpinPage(BM_BufferPool * const bm, BM_PageHandle * const page);
extern RC forcePage(BM_BufferPool * const bm, BM_PageHandle * const page);
extern RC pinPage(BM_BufferPool * const bm, BM_PageHandle * const page,
		const PageNumber pageNum);

// Statistics Interface
PageNumber *getFrameContents(BM_BufferPool * const bm);
bool *getDirtyFlags(BM_BufferPool * const bm);
int *getFixCounts(BM_BufferPool * const bm);
int getNumReadIO(BM_BufferPool * const bm);
int getNumWriteIO(BM_BufferPool * const bm);
float getPageHitCount(BM_BufferPool * const bm);
float getPageHitRatio(BM_BufferPool * const bm);

extern void printIOStat(BM_BufferPool * const bm);
extern bool writeNewBlocks(BM_BufferPool * const bm, PageNumber num);
extern void printDebugInfo(BM_BufferPool * const bm);

#endif
