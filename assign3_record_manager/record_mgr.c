#include <stdio.h>
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "record_mgr.h"
#include <stdlib.h>
#include <sys/stat.h>
#include <string.h>
//actual structure of the record manager
typedef struct RM
{   
    //This helps us access the buffer pool insede the record manager
	BM_PageHandle pageHandle;	
	BM_BufferPool bufferPool;
    // Stores the record id
	RID recordID;
    //The number of tuples present is stored here
	int tuplesCount;
	//The conditon used for scanning the records is stored in condition
	Expr *condition;
	//It tells us where we could find a nearest free slot in the pages
	int freePage;
	//This gives us the number of scanned records
	int scanCount;
} RM;

RM *Record_mgr;

//Find a free slot in a page 
int findFreeSlot(char *data, int record_size)
{
	int i=0;
    int number_of_slots;
    number_of_slots = PAGE_SIZE / record_size; 

	while(i < number_of_slots)
    {
		if (data[i * record_size] != '+')
			return i;
        else
        {
            i++;
        }
    }
    return -1;
}

//Initiliaze Record Manager
RC initRecordManager (void *mgmtData)
{
	//we need to Initiliaze Storage Manager
	initStorageManager();
	return RC_OK;
}

//Shutdown Record Manager
RC shutdownRecordManager ()
{
    //We set the Record_mgr to Null and free the Record_mgr to shut it down
	Record_mgr = NULL;
	free(Record_mgr);
	return RC_OK;
}

//This function creates a table qqwith name and schema specified
RC createTable (char *name, Schema *schema)
{   
    SM_FileHandle *fHandle;
    //Set the data size
    char data[PAGE_SIZE];
	char *pageHandle = data;
	//Allocated required memory
	Record_mgr = (RM*) calloc(1,sizeof(RM));

	//Set buffer pool with max_pages and replacement strategy as LRU
	initBufferPool(&Record_mgr->bufferPool, name, MAX_PAGES, RS_LRU, NULL);
    
    //0th page for meta data
	* (int*)pageHandle = 0; 
	pageHandle = pageHandle + sizeof(int);
	
	//We set the first page to 1 as the 0th page is used for meta dats storage
	*(int*)pageHandle = 1;
	pageHandle = pageHandle + sizeof(int);

	//Set the number of attributes used (max is set to 15)
	*(int*)pageHandle = schema->numAttr;
	pageHandle = pageHandle + sizeof(int); 

	//After setting attributes, we set its key size
	*(int*)pageHandle = schema->keySize;
	pageHandle = pageHandle + sizeof(int);
	
    int i=0;
	while(i < schema->numAttr)
    	{
        //For all attributes we set its attribute name, data type, and the total size of attribute
		//Set attribute name
       	strncpy(pageHandle, schema->attrNames[i], ATTRIBUTE_SIZE);
	    pageHandle = pageHandle + ATTRIBUTE_SIZE;
	
		//Set data type
		*(int*)pageHandle = (int)schema->dataTypes[i];
       	pageHandle = pageHandle + sizeof(int);

		// Set the total size of attribute
	   	*(int*)pageHandle = (int) schema->typeLength[i];
	   	pageHandle = pageHandle + sizeof(int);
        i++;
    	}
		
	//Using the storage manager fuctions to create open, write and close file.
    int return_status;
    //Create page file and we return the status if any error occurs
    return_status = createPageFile(name);
	if(return_status != RC_OK)
		return return_status;
		
	//Open page file and we return the status if any error occurs
    return_status = openPageFile(name, fHandle);
	if(return_status != RC_OK)
		return return_status;
		
	//Write page file to the 0th location (we store meta data) and we return the status if any error occurs
    return_status = writeBlock(0, fHandle, data);
	if(return_status != RC_OK)
		return return_status;
		
	//Open page file and we return the status if any error occurs
    return_status = closePageFile(fHandle);
	if(return_status != RC_OK)
		return return_status;

    //We return RC_OK if everything goes well
	return RC_OK;
}

//The function opens the table created
RC openTable (RM_TableData *rel, char *name)
{ 
	//Set the name and meta data of table using the relation
	rel->name = name;
	rel->mgmtData = Record_mgr;
	
	//As we need to access the page we need to put it in buffer pull, hence we pin the page
	pinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle, 0);
	
    SM_PageHandle pHandle;   
	pHandle = (char*) Record_mgr->pageHandle.data;
	
	//Set the tuple count
	Record_mgr->tuplesCount = *(int*)pHandle;
	pHandle = pHandle + sizeof(int);

    //set the number of attributes used
    int No_of_attributes;
    No_of_attributes = *(int*)pHandle;
	pHandle = pHandle + sizeof(int);

	//Set the free page for record manager
	Record_mgr->freePage= *(int*) pHandle;
    pHandle = pHandle + sizeof(int);
 	
	Schema *table_schema;

	//Reserving space for table_schema
	table_schema = (Schema*)calloc(1,sizeof(Schema));
    
	//Allocating memory and set the variables of table_schema
	table_schema->numAttr = No_of_attributes;

    int k=0;
	while(k < No_of_attributes)
    {   //Reserve space for each attributes name
        table_schema->attrNames[k]= (char*) calloc(1, ATTRIBUTE_SIZE);
        k++;
    }

	table_schema->attrNames = (char**) calloc(No_of_attributes,sizeof(char*));
	table_schema->dataTypes = (DataType*) calloc(No_of_attributes,sizeof(DataType));
	table_schema->typeLength = (int*) calloc(No_of_attributes,sizeof(int));

	k=0;  
	while(k < table_schema->numAttr)
    	{
		//For all attributes we set its attribute name, data type, and the total size of attribute
		strncpy(table_schema->attrNames[k], pHandle, ATTRIBUTE_SIZE);
		pHandle = pHandle + ATTRIBUTE_SIZE;
	   
		table_schema->dataTypes[k]= *(int*) pHandle;
		pHandle = pHandle + sizeof(int);

		table_schema->typeLength[k]= *(int*)pHandle;
		pHandle = pHandle + sizeof(int);
        k++;
	    }
	//Set schema of relation as described above
	rel->schema = table_schema;	

	//As we have finished using the particular page we remove the page from bufferpool hence unpin the page
	unpinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle);

	//wite the particular page back to the disk
	forcePage(&Record_mgr->bufferPool, &Record_mgr->pageHandle);

    //We return RC_OK if everything goes well
	return RC_OK;
}   

// Closes the desired table
RC closeTable (RM_TableData *rel)
{
	//Perserve the tables meta data, as it would be needed
	RM *Record_mgr = rel->mgmtData;
	//Free all the resources associated with bufferpool
	shutdownBufferPool(&Record_mgr->bufferPool);
	return RC_OK;
}

//Delete's the desired table
RC deleteTable (char *name)
{
	//Remove(destroy) that particular page from memory
	destroyPageFile(name);
	return RC_OK;
}

//To get the number of tuples in desired table
int getNumTuples (RM_TableData *rel)
{
	//As we have stored the tupleCount variable in mgmtData, we access that and return tuplesCount
	RM *Record_mgr = rel->mgmtData;
	return Record_mgr->tuplesCount;
}


// ******** RECORD FUNCTIONS ******** //

// This function inserts a new record in the table referenced by "rel" and updates the 'record' parameter with the Record ID of he newly inserted record
extern RC insertRecord (RM_TableData *rel, Record *record)
{
	// Retrieving our meta data stored in the table
	RM *recordManager = rel->mgmtData;	
	
	// Setting the Record ID for this record
	RID *recordID = &record->id; 
	
	char *data, *slotPointer;
	
	// Getting the size in bytes needed to store on record for the given schema
	int recordSize = getRecordSize(rel->schema);
	
	// Setting first free page to the current page
	recordID->page = recordManager->freePage;

	// Pinning page i.e. telling Buffer Manager that we are using this page
	pinPage(&recordManager->bufferPool, &recordManager->pageHandle, recordID->page);
	
	// Setting the data to initial position of record's data
	data = recordManager->pageHandle.data;
	
	// Getting a free slot using our custom function
	recordID->slot = findFreeSlot(data, recordSize);

	while(recordID->slot == -1)
	{
		// If the pinned page doesn't have a free slot then unpin that page
		unpinPage(&recordManager->bufferPool, &recordManager->pageHandle);	
		
		// Incrementing page
		recordID->page++;
		
		// Bring the new page into the BUffer Pool using Buffer Manager
		pinPage(&recordManager->bufferPool, &recordManager->pageHandle, recordID->page);
		
		// Setting the data to initial position of record's data		
		data = recordManager->pageHandle.data;

		// Again checking for a free slot using our custom function
		recordID->slot = findFreeSlot(data, recordSize);
	}
	
	slotPointer = data;
	
	// Mark page dirty to notify that this page was modified
	markDirty(&recordManager->bufferPool, &recordManager->pageHandle);
	
	// Calculation slot starting position
	slotPointer = slotPointer + (recordID->slot * recordSize);

	// Appending '+' as tombstone to indicate this is a new record and should be removed if space is lesss
	*slotPointer = '+';

	// Copy the record's data to the memory location pointed by slotPointer
	memcpy(++slotPointer, record->data + 1, recordSize - 1);

	// Unpinning a page i.e. removing a page from the BUffer Pool
	unpinPage(&recordManager->bufferPool, &recordManager->pageHandle);
	
	// Incrementing count of tuples
	recordManager->tuplesCount++;
	
	// Pinback the page	
	pinPage(&recordManager->bufferPool, &recordManager->pageHandle, 0);

	return RC_OK;
}


// dealing with schemas
// This function creates a new schema
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
	// Allocate memory space to schema
	Schema *schema = (Schema *) malloc(sizeof(Schema));
	// Set the Number of Attributes in the new schema	
	schema->numAttr = numAttr;
	// Set the Attribute Names in the new schema
	schema->attrNames = attrNames;
	// Set the Data Type of the Attributes in the new schema
	schema->dataTypes = dataTypes;
	// Set the Type Length of the Attributes i.e. STRING size  in the new schema
	schema->typeLength = typeLength;
	// Set the Key Size  in the new schema
	schema->keySize = keySize;
	// Set the Key Attributes  in the new schema
	schema->keyAttrs = keys;

	return schema; 
}
// This function returns the record size of the schema referenced by "schema"
extern int getRecordSize (Schema *schema)
{
	int size = 0, i; // offset set to zero
	
	// Iterating through all the attributes in the schema
	for(i = 0; i < schema->numAttr; i++)
	{
		switch(schema->dataTypes[i])
		{
			// Switch depending on DATA TYPE of the ATTRIBUTE
			case DT_STRING:
				// If attribute is STRING then size = typeLength (Defined Length of STRING)
				size = size + schema->typeLength[i];
				break;
			case DT_INT:
				// If attribute is INTEGER, then add size of INT
				size = size + sizeof(int);
				break;
			case DT_FLOAT:
				// If attribite is FLOAT, then add size of FLOAT
				size = size + sizeof(float);
				break;
			case DT_BOOL:
				// If attribite is BOOLEAN, then add size of BOOLEAN
				size = size + sizeof(bool);
				break;
		}
	}
	return ++size;
}
// dealing with records and attribute values

// ******** DEALING WITH RECORDS AND ATTRIBUTE VALUES ******** //

// This function creates a new record in the schema referenced by "schema"
extern RC createRecord (Record **record, Schema *schema)
{
	// Allocate some memory space for the new record
	Record *newRecord = (Record*) malloc(sizeof(Record));
	
	// Retrieve the record size
	int recordSize = getRecordSize(schema);

	// Allocate some memory space for the data of new record    
	newRecord->data= (char*) malloc(recordSize);

	// Setting page and slot position. -1 because this is a new record and we don't know anything about the position
	newRecord->id.page = newRecord->id.slot = -1;

	// Getting the starting position in memory of the record's data
	char *dataPointer = newRecord->data;
	
	// '-' is used for Tombstone mechanism. We set it to '-' because the record is empty.
	*dataPointer = '-';
	
	// Append '\0' which means NULL in C to the record after tombstone. ++ because we need to move the position by one before adding NULL
	*(++dataPointer) = '\0';

	// Set the newly created record to 'record' which passed as argument
	*record = newRecord;

	return RC_OK;
}

// This function sets the offset (in bytes) from initial position to the specified attribute of the record into the 'result' parameter passed through the function
RC attrOffset (Schema *schema, int attrNum, int *result)
{
	int i;
	*result = 1;

	// Iterating through all the attributes in the schema
	for(i = 0; i < attrNum; i++)
	{
		// Switch depending on DATA TYPE of the ATTRIBUTE
		switch (schema->dataTypes[i])
		{
			// Switch depending on DATA TYPE of the ATTRIBUTE
			case DT_STRING:
				// If attribute is STRING then size = typeLength (Defined Length of STRING)
				*result = *result + schema->typeLength[i];
				break;
			case DT_INT:
				// If attribute is INTEGER, then add size of INT
				*result = *result + sizeof(int);
				break;
			case DT_FLOAT:
				// If attribite is FLOAT, then add size of FLOAT
				*result = *result + sizeof(float);
				break;
			case DT_BOOL:
				// If attribite is BOOLEAN, then add size of BOOLEAN
				*result = *result + sizeof(bool);
				break;
		}
	}
	return RC_OK;
}
// This function removes the record from the memory.
extern RC freeRecord (Record *record)
{
	// De-allocating memory space allocated to record and freeing up that space
	free(record);
	return RC_OK;
}

// This function retrieves an attribute from the given record in the specified schema
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
	int offset = 0;

	// Getting the ofset value of attributes depending on the attribute number
	attrOffset(schema, attrNum, &offset);

	// Allocating memory space for the Value data structure where the attribute values will be stored
	Value *attribute = (Value*) malloc(sizeof(Value));

	// Getting the starting position of record's data in memory
	char *dataPointer = record->data;
	
	// Adding offset to the starting position
	dataPointer = dataPointer + offset;

	// If attrNum = 1
	schema->dataTypes[attrNum] = (attrNum == 1) ? 1 : schema->dataTypes[attrNum];
	
	// Retrieve attribute's value depending on attribute's data type
	switch(schema->dataTypes[attrNum])
	{
		case DT_STRING:
		{
     			// Getting attribute value from an attribute of type STRING
			int length = schema->typeLength[attrNum];
			// Allocate space for string hving size - 'length'
			attribute->v.stringV = (char *) malloc(length + 1);

			// Copying string to location pointed by dataPointer and appending '\0' which denotes end of string in C
			strncpy(attribute->v.stringV, dataPointer, length);
			attribute->v.stringV[length] = '\0';
			attribute->dt = DT_STRING;
      			break;
		}

		case DT_INT:
		{
			// Getting attribute value from an attribute of type INTEGER
			int value = 0;
			memcpy(&value, dataPointer, sizeof(int));
			attribute->v.intV = value;
			attribute->dt = DT_INT;
      			break;
		}
    
		case DT_FLOAT:
		{
			// Getting attribute value from an attribute of type FLOAT
	  		float value;
	  		memcpy(&value, dataPointer, sizeof(float));
	  		attribute->v.floatV = value;
			attribute->dt = DT_FLOAT;
			break;
		}

		case DT_BOOL:
		{
			// Getting attribute value from an attribute of type BOOLEAN
			bool value;
			memcpy(&value,dataPointer, sizeof(bool));
			attribute->v.boolV = value;
			attribute->dt = DT_BOOL;
      			break;
		}

		default:
			printf("Serializer not defined for the given datatype. \n");
			break;
	}

	*value = attribute;
	return RC_OK;
}
// This function sets the attribute value in the record in the specified schema
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
	int offset = 0;

	// Getting the ofset value of attributes depending on the attribute number
	attrOffset(schema, attrNum, &offset);

	// Getting the starting position of record's data in memory
	char *dataPointer = record->data;
	
	// Adding offset to the starting position
	dataPointer = dataPointer + offset;
		
	switch(schema->dataTypes[attrNum])
	{
		case DT_STRING:
		{
			// Setting attribute value of an attribute of type STRING
			// Getting the legeth of the string as defined while creating the schema
			int length = schema->typeLength[attrNum];

			// Copying attribute's value to the location pointed by record's data (dataPointer)
			strncpy(dataPointer, value->v.stringV, length);
			dataPointer = dataPointer + schema->typeLength[attrNum];
		  	break;
		}

		case DT_INT:
		{
			// Setting attribute value of an attribute of type INTEGER
			*(int *) dataPointer = value->v.intV;	  
			dataPointer = dataPointer + sizeof(int);
		  	break;
		}
		
		case DT_FLOAT:
		{
			// Setting attribute value of an attribute of type FLOAT
			*(float *) dataPointer = value->v.floatV;
			dataPointer = dataPointer + sizeof(float);
			break;
		}
		
		case DT_BOOL:
		{
			// Setting attribute value of an attribute of type STRING
			*(bool *) dataPointer = value->v.boolV;
			dataPointer = dataPointer + sizeof(bool);
			break;
		}

		default:
			printf("Serializer not defined for the given datatype. \n");
			break;
	}			
	return RC_OK;
}

// ******** SCAN FUNCTIONS ******** //

// This function scans all the records using the condition
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
	// Checking if scan condition (test expression) is present
	if (cond == NULL)
	{
		return RC_SCAN_CONDITION_NOT_FOUND;
	}

	// Open the table in memory
	openTable(rel, "ScanTable");

    	RM *scanManager;
		RM *tableManager;

	// Allocating some memory to the scanManager
    	scanManager = (RM*) malloc(sizeof(RM));
    	
	// Setting the scan's meta data to our meta data
    	scan->mgmtData = scanManager;
    	
	// 1 to start scan from the first page
    	scanManager->recordID.page = 1;
    	
	// 0 to start scan from the first slot	
	scanManager->recordID.slot = 0;
	
	// 0 because this just initializing the scan. No records have been scanned yet    	
	scanManager->scanCount = 0;

	// Setting the scan condition
    	scanManager->condition = cond;
    	
	// Setting the our meta data to the table's meta data
    	tableManager = rel->mgmtData;

	// Setting the tuple count
    	tableManager->tuplesCount = ATTRIBUTE_SIZE;

	// Setting the scan's table i.e. the table which has to be scanned using the specified condition
    	scan->rel= rel;

	return RC_OK;
}

// This function scans each record in the table and stores the result record (record satisfying the condition)
// in the location pointed by  'record'.
extern RC next (RM_ScanHandle *scan, Record *record)
{
	// Initiliazing scan data
	RM *scanManager = scan->mgmtData;
	RM *tableManager = scan->rel->mgmtData;
    	Schema *schema = scan->rel->schema;
	
	// Checking if scan condition (test expression) is present
	if (scanManager->condition == NULL)
	{
		return RC_SCAN_CONDITION_NOT_FOUND;
	}

	Value *result = (Value *) malloc(sizeof(Value));
   
	char *data;
   	
	// Getting record size of the schema
	int recordSize = getRecordSize(schema);

	// Calculating Total number of slots
	int totalSlots = PAGE_SIZE / recordSize;

	// Getting Scan Count
	int scanCount = scanManager->scanCount;

	// Getting tuples count of the table
	int tuplesCount = tableManager->tuplesCount;

	// Checking if the table contains tuples. If the tables doesn't have tuple, then return respective message code
	if (tuplesCount == 0)
		return RC_RM_NO_MORE_TUPLES;

	// Iterate through the tuples
	while(scanCount <= tuplesCount)
	{  
		// If all the tuples have been scanned, execute this block
		if (scanCount <= 0)
		{
			// printf("INSIDE If scanCount <= 0 \n");
			// Set PAGE and SLOT to first position
			scanManager->recordID.page = 1;
			scanManager->recordID.slot = 0;
		}
		else
		{
			// printf("INSIDE Else scanCount <= 0 \n");
			scanManager->recordID.slot++;

			// If all the slots have been scanned execute this block
			if(scanManager->recordID.slot >= totalSlots)
			{
				scanManager->recordID.slot = 0;
				scanManager->recordID.page++;
			}
		}

		// Pinning the page i.e. putting the page in buffer pool
		pinPage(&tableManager->bufferPool, &scanManager->pageHandle, scanManager->recordID.page);
			
		// Retrieving the data of the page			
		data = scanManager->pageHandle.data;

		// Calulate the data location from record's slot and record size
		data = data + (scanManager->recordID.slot * recordSize);
		
		// Set the record's slot and page to scan manager's slot and page
		record->id.page = scanManager->recordID.page;
		record->id.slot = scanManager->recordID.slot;

		// Intialize the record data's first location
		char *dataPointer = record->data;

		// '-' is used for Tombstone mechanism.
		*dataPointer = '-';
		
		memcpy(++dataPointer, data + 1, recordSize - 1);

		// Increment scan count because we have scanned one record
		scanManager->scanCount++;
		scanCount++;

		// Test the record for the specified condition (test expression)
		evalExpr(record, schema, scanManager->condition, &result); 

		// v.boolV is TRUE if the record satisfies the condition
		if(result->v.boolV == TRUE)
		{
			// Unpin the page i.e. remove it from the buffer pool.
			unpinPage(&tableManager->bufferPool, &scanManager->pageHandle);
			// Return SUCCESS			
			return RC_OK;
		}
	}
	
	// Unpin the page i.e. remove it from the buffer pool.
	unpinPage(&tableManager->bufferPool, &scanManager->pageHandle);
	
	// Reset the Scan Manager's values
	scanManager->recordID.page = 1;
	scanManager->recordID.slot = 0;
	scanManager->scanCount = 0;
	
	// None of the tuple satisfy the condition and there are no more tuples to scan
	return RC_RM_NO_MORE_TUPLES;
}

// This function closes the scan operation.
extern RC closeScan (RM_ScanHandle *scan)
{
	RM *scanManager = scan->mgmtData;
	RM *recordManager = scan->rel->mgmtData;

	// Check if scan was incomplete
	if(scanManager->scanCount > 0)
	{
		// Unpin the page i.e. remove it from the buffer pool.
		unpinPage(&recordManager->bufferPool, &scanManager->pageHandle);
		
		// Reset the Scan Manager's values
		scanManager->scanCount = 0;
		scanManager->recordID.page = 1;
		scanManager->recordID.slot = 0;
	}
	
	// De-allocate all the memory space allocated to the scans's meta data (our custom structure)
    	scan->mgmtData = NULL;
    	free(scan->mgmtData);  
	
	return RC_OK;
}
