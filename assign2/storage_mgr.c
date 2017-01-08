#include "storage_mgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define PRIVATE static
#define META_FIELD_SIZE 10

int access(const char *, int);
void updateMetaData(SM_FileHandle *);

PRIVATE RC readBlockGeneric(int, SM_FileHandle *, SM_PageHandle);
PRIVATE RC writeBlockGeneric(int, SM_FileHandle *, SM_PageHandle);

static short metaChanged = 0;

/**
 *	Initialize Storage Manager.
 */
void initStorageManager(void) {
	printf("\nStorage Manager is ready");
}

/**
 *	Creates a page file with name filename.
 *
 *	filename = name of the page file to be created
 */
RC createPageFile(char *filename) {
	//Create a file
	//If already exists then existing contents will be discarded
	FILE *fp = fopen(filename, "w");

	if (fp == NULL)
		THROW(RC_FILE_NOT_FOUND, "File not found");

	//Writes metadata field in first block. Metadata contains total number of pages in file.
	//Metadata block is of fixed size META_FIELD_SIZE.
	char *ph = (SM_PageHandle) malloc(META_FIELD_SIZE);

	memset(ph, '\0', META_FIELD_SIZE);
	sprintf(ph, "%i", 1);
	if (fwrite(ph, 1, META_FIELD_SIZE, fp) != META_FIELD_SIZE) {
		fclose(fp);
		THROW(RC_WRITE_FAILED, "Unable to write metadata field to file");
	}
	free(ph);

	ph = (SM_PageHandle) malloc(PAGE_SIZE);
	memset(ph, '\0', PAGE_SIZE);
	if (fwrite(ph, 1, PAGE_SIZE, fp) != PAGE_SIZE) {
		fclose(fp);
		THROW(RC_WRITE_FAILED, "Unable to create file");
	}
	free(ph);

	//Close the opened file
	fclose(fp);

	return RC_OK;
}

/**
 *	Opens a pagefile named filename for read/write operations.
 *
 *	filename = name of the page file to be opened
 *	fHandle = page file handle
 */
RC openPageFile(char *filename, SM_FileHandle *fHandle) {
	// the file must exist
	if (access(filename, F_OK) == -1)
		THROW(RC_FILE_NOT_FOUND, "File not found");

	//Pagefile opened in read + update mode
	FILE *fp = fopen(filename, "r+");
	if (fp == NULL)
		THROW(RC_FILE_NOT_FOUND, "Error opening file");

	char numPages[META_FIELD_SIZE];
	memset(numPages, '\0', META_FIELD_SIZE);

	if ((fread(numPages, 1, META_FIELD_SIZE, fp)) != META_FIELD_SIZE) {
		THROW(RC_READ_FAILED, "Unable to read metadata field from file");
	}

	//Initialize file handle fields
	fHandle->fileName = filename;
	fHandle->curPagePos = 0;
	fHandle->mgmtInfo = (FILE*) fp;
	//Set total number of pages from metadata from first block
	fHandle->totalNumPages = atoi(numPages);

	return RC_OK;
}

/**
 * 	Closes page file.
 *
 * 	fHandle = page file handle
 */
RC closePageFile(SM_FileHandle *fHandle) {
	if (fHandle == NULL)
		THROW(RC_FILE_HANDLE_NOT_INIT, "Page file handle not initialized");

	FILE *fp = (FILE*) fHandle->mgmtInfo;
	//Write updated metadata field to disk only at end
	if (metaChanged) {
		updateMetaData(fHandle);
		metaChanged = 0;
	}
	fsync(fileno(fp));
	if (fclose(fp) != 0) {
		THROW(RC_FILE_CLOSE_FAILED, "Failed to close pagefile");
	}

	return RC_OK;
}

/**
 * 	Deletes page file.
 *
 * 	fileName = name of page file to be deleted
 */
RC destroyPageFile(char *fileName) {
	if (remove(fileName) != 0) {
		THROW(RC_FILE_DELETE_FAILED, "Failed to delete pagefile");
	}

	return RC_OK;
}

/**
 *	Returns current file block that file handle points to.
 *
 *	fHandle = page file handle to be queried for current block position
 */
int getBlockPos(SM_FileHandle *fHandle) {
	if (fHandle == NULL)
		THROW(RC_FILE_HANDLE_NOT_INIT, "Page file handle not initialized");

	// curPagePos represents the page index and it starts from 0
	return fHandle->curPagePos;
}

/**
 *	Private function to read pageNumth block from page file.
 *
 *	pageNum = page file block no. to be read
 *	fHandle = page file handle
 *	memPage = buffer in which block data read is to be returned
 */
PRIVATE RC readBlockGeneric(int pageNum, SM_FileHandle *fHandle,
		SM_PageHandle memPage) {

	//Check if page requested does exist
	if (fHandle->totalNumPages < (pageNum + 1))
		THROW(RC_READ_NON_EXISTING_PAGE, "Attempt to read non-existing page");

	fHandle->curPagePos = pageNum;

	FILE *fp = (FILE*) fHandle->mgmtInfo;

	fflush(fp);

	long int currentPos = ftell(fp);
	long int newPos = ((pageNum) * PAGE_SIZE) + META_FIELD_SIZE;

	if (newPos != currentPos)
		fseek(fp, (newPos - currentPos), SEEK_CUR);

	char buf[PAGE_SIZE];
	if ((fread(buf, 1, PAGE_SIZE, fp)) != PAGE_SIZE) {
		THROW(RC_READ_FAILED, "Unable to read from specified block");
	}

	memset(memPage, '\0', PAGE_SIZE);
	strcpy(memPage, buf);

	return RC_OK;
}

/**
 *	Read pageNumth block from page file.
 *
 *	pageNum = page file block no. to be read
 *	fHandle = page file handle
 *	memPage = buffer in which block data read is to be returned
 */
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	//Check if page file handle is init
	if (fHandle == NULL)
		THROW(RC_FILE_HANDLE_NOT_INIT, "Page file handle not initialized");

	return readBlockGeneric(pageNum, fHandle, memPage);
}

/**
 * 	Reads first block of page file
 *
 *	fHandle = page file handle
 *	memPage = buffer in which block data read is to be returned
 */
RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	//Check if page file handle is init
	if (fHandle == NULL)
		THROW(RC_FILE_HANDLE_NOT_INIT, "Page file handle not initialized");

	return readBlockGeneric(0, fHandle, memPage);
}

/**
 * 	Reads previous block of current block pointed by fHandle, then sets curPagePos to that block.
 *
 *	fHandle = page file handle
 *	memPage = buffer in which block data read is to be returned
 */
RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	//Check if page file handle is init
	if (fHandle == NULL)
		THROW(RC_FILE_HANDLE_NOT_INIT, "Page file handle not initialized");

	//Check if page requested is valid. If current page is 0, previous page will not exist.
	if (fHandle->curPagePos == 0)
		THROW(RC_READ_NON_EXISTING_PAGE, "Attempt to read non-existing page");

	return readBlockGeneric(fHandle->curPagePos - 1, fHandle, memPage);
}

/**
 *	Reads current block pointed by fHandle
 *
 *	fHandle = page file handle
 *	memPage = buffer in which block data read is to be returned
 */
RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	//Check if page file handle is init
	if (fHandle == NULL)
		THROW(RC_FILE_HANDLE_NOT_INIT, "Page file handle not initialized");

	return readBlockGeneric(fHandle->curPagePos, fHandle, memPage);
}

/**
 * 	Reads next block of current block pointed by fHandle, then sets curPagePos to that block.
 *
 *	fHandle = page file handle
 *	memPage = buffer in which block data read is to be returned
 */
RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	//Check if page file handle is init
	if (fHandle == NULL)
		THROW(RC_FILE_HANDLE_NOT_INIT, "Page file handle not initialized");

	//Check if page requested is valid. If current page is last page, there is no next page.
	if ((fHandle->curPagePos + 1) == fHandle->totalNumPages)
		THROW(RC_READ_NON_EXISTING_PAGE, "Attempt to read non-existing page");

	return readBlockGeneric(fHandle->curPagePos + 1, fHandle, memPage);
}

/**
 * 	Reads last block of the page file, then sets curPagePos to that block.
 *
 *	fHandle = page file handle
 *	memPage = buffer in which block data read is to be returned
 */
RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	//Check if page file handle is init
	if (fHandle == NULL)
		THROW(RC_FILE_HANDLE_NOT_INIT, "Page file handle not initialized");

	return readBlockGeneric(fHandle->totalNumPages - 1, fHandle, memPage);
}

/**
 * 	Private function to writes data to page file blocks
 *
 *	pageNum = index of the block to which data is to be written
 *	fHandle = page file handle
 *	memPage = buffer containing data to be written to block
 */
PRIVATE RC writeBlockGeneric(int pageNum, SM_FileHandle *fHandle,
		SM_PageHandle memPage) {
	//TODO Add memPage size check

	//Check if page file handle is init
	if (fHandle == NULL)
		THROW(RC_FILE_HANDLE_NOT_INIT, "Page file handle not initialized");

	//Check if destination page index is valid
	if (pageNum < 0 || pageNum >= fHandle->totalNumPages)
		THROW(RC_WRITE_NON_EXISTING_PAGE,
				"Attempt to write to non existing page");

	FILE *fp = (FILE*) fHandle->mgmtInfo;

	if (fp) {
		//Update the current page
		fHandle->curPagePos = pageNum;
		int bytes_written = 0;

		long int currentPos = ftell(fp);
		long int newPos = ((pageNum) * PAGE_SIZE) + META_FIELD_SIZE;

		if (newPos != currentPos)
			fseek(fp, (newPos - currentPos), SEEK_CUR);

		bytes_written = fwrite(memPage, 1, strlen(memPage), fp);
		if (bytes_written != strlen(memPage)) {
			fclose(fp);
			THROW(RC_WRITE_FAILED, "Unable to write data to block");
		}
		fflush(fp);
		return RC_OK;
	} else {
		THROW(RC_WRITE_FAILED, "Invalid File Pointer");
	}
}

/**
 * 	Writes data pointed by memory page memPage to page of index pageNum, then sets curPagePos to that block,
 * 	then updates curPagePos
 *
 *	pageNum = index of the block to which data is to be written
 *	fHandle = page file handle
 *	memPage = buffer containing data to be written to block
 */
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
	return writeBlockGeneric(pageNum, fHandle, memPage);
}

/**
 * 	Writes data pointed by memory page memPage to the page file block pointed by curPagePos
 *
 *	fHandle = page file handle
 *	memPage = buffer containing data to be written to block
 */
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	return writeBlockGeneric(fHandle->curPagePos, fHandle, memPage);
}

/**
 *	Internal utility function to append block
 *
 *	fHandle = page file handle
 *	memPage = page content to be written
 */
PRIVATE RC appendEmptyBlockGeneric(SM_FileHandle *fHandle,
		SM_PageHandle memPage) {
	FILE *fp = (FILE*) fHandle->mgmtInfo;

	if (fp) {
		//Move the file pointer to the end of the file
		if (fseek(fp, 0L, SEEK_END) == -1) {
			THROW(RC_WRITE_FAILED, "Unable to append an empty block");
		}

		char *ph;
		if (memPage == NULL) {
			ph = (SM_PageHandle) malloc(PAGE_SIZE);
			memset(ph, '\0', PAGE_SIZE);
		} else {
			ph = memPage;
		}

		if (fwrite(ph, 1, PAGE_SIZE, fp) != PAGE_SIZE) {
			fclose(fp);
			THROW(RC_WRITE_FAILED, "Unable to write to new block");
		}

		//Just mark that metadata needs to be written back to file later
		metaChanged = 1;

		if (memPage == NULL) {
			free(ph);
		}
		fflush(fp);

		//Update total page count
		fHandle->totalNumPages = (fHandle->totalNumPages) + 1;

		//Update current page number
		fHandle->curPagePos++;
	} else {
		THROW(RC_WRITE_FAILED, "Invalid File Pointer");
	}

	return RC_OK;
}

/**
 *	Adds an empty block of size PAGE_SIZE at the end of the page file
 *	and writes memPage data to it.
 *
 *	fHandle = page file handle
 *	memPage = page content to be written
 */
RC appendEmptyBlockData(SM_FileHandle *fHandle, SM_PageHandle memPage) {
	//Check if page file handle is init
	if (fHandle == NULL)
		THROW(RC_FILE_HANDLE_NOT_INIT, "Page file handle not initialized");

	return appendEmptyBlockGeneric(fHandle, memPage);
}

/**
 *	Adds an empty block of size PAGE_SIZE at the end of the page file. That block is initialized with NULL.
 *
 *	fHandle = page file handle
 */
RC appendEmptyBlock(SM_FileHandle *fHandle) {
	//Check if page file handle is init
	if (fHandle == NULL)
		THROW(RC_FILE_HANDLE_NOT_INIT, "Page file handle not initialized");

	return appendEmptyBlockGeneric(fHandle, NULL);
}

/**
 *	Ensures that page file has block count of at least numberOfPages
 *
 *	numberOfPages = minimum number of pages that the page file must have
 *	fHandle = page file handle
 */
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
	//Check if page file handle is init
	if (fHandle == NULL)
		THROW(RC_FILE_HANDLE_NOT_INIT, "Page file handle not initialized");

	if (fHandle->totalNumPages < numberOfPages) {
		FILE *fp = (FILE*) fHandle->mgmtInfo;

		if (fp) {
			int newAddtlnPages = numberOfPages - fHandle->totalNumPages;

			// move the file pointer to the end of the file
			if (fseek(fp, 0L, SEEK_END) == -1) {
				THROW(RC_WRITE_FAILED, "Unable to append an empty block");
			}

			char *ph = (SM_PageHandle) malloc(newAddtlnPages * PAGE_SIZE);
			memset(ph, '\0', newAddtlnPages * PAGE_SIZE);

			if (fwrite(ph, 1, (newAddtlnPages * PAGE_SIZE), fp)
					!= (newAddtlnPages * PAGE_SIZE)) {
				fclose(fp);
				THROW(RC_WRITE_FAILED, "Unable to write to new block");
			}

			//Just mark that metadata needs to be written back to file later
			metaChanged = 1;

			free(ph);
			fflush(fp);

			//Update the total number of pages
			fHandle->totalNumPages = numberOfPages;

			//Update current page number
			fHandle->curPagePos = numberOfPages - 1;

			return RC_OK;
		} else {
			THROW(RC_WRITE_FAILED, "Invalid File Pointer");
		}
	}

	return RC_OK;
}

/**
 * 	Internal function to update page count metadata.
 *
 * 	fHandle = page file handle
 */
void updateMetaData(SM_FileHandle *fHandle) {
	char *ph = (SM_PageHandle) malloc(META_FIELD_SIZE);
	memset(ph, '\0', META_FIELD_SIZE);
	sprintf(ph, "%i", fHandle->totalNumPages);
	FILE *fp = (FILE*) fHandle->mgmtInfo;
	fseek(fp, 0L, SEEK_SET);
	fwrite(ph, 1, META_FIELD_SIZE, fp);
	free(ph);
}
