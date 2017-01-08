/*
 * record_mgr.c
 *
 *  Created on: Oct 25, 2015
 *      Author: heisenberg
 */

#include "dberror.h"
#include "storage_mgr.h"

/**
 * Initializes Record Manager
 *
 */
RC initRecordManager(void *mgmtData) {

	//Init storage manager for pagefile
	initStorageManager();
	//All OK
	return RC_OK;
}

/**
 * Shuts down Record Manager
 */
RC shutdownRecordManager() {
	//All OK
	return RC_OK;
}
