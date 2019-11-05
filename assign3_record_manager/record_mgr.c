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
int freeslot_finder(char *data, int record_size)
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

// table and manager

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
	RC a = unpinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle);
	printf("rsult -> %i\n",a);
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

// handling records in a table

// Insets record into desired table
RC insertRecord (RM_TableData *rel, Record *record)
{
	RM *Record_mgr = rel->mgmtData;	
	//Assign record id
	RID *R_id = &record->id; 
	char *data;
	// Calculate the size needed to store the record
	int R_size = getRecordSize(rel->schema);
	//Initiaze the page to the free page (to store the record)
	R_id->page = Record_mgr->freePage;
	//As we are using the current page to store the record, hence the page should be marked pinned. 
	pinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle, R_id->page);
	data = Record_mgr->pageHandle.data;
	
	//Search for the free space using the freeslot_finder function specified
	R_id->slot = freeslot_finder(data, R_size);

	/*If R_id->slot is equal to -1, then we need to find a free slot in the upcoming pages. For this we need to unpin the page currently in the buffer bool,
	add a new page to buffer pool, pin the added page and then check for free slot in this page. We repeat this untill we find a free slot*/
	while(R_id->slot == -1)
	{
		unpinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle);	
		R_id->page++;
		pinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle, R_id->page);		
		data = Record_mgr->pageHandle.data;
		R_id->slot = freeslot_finder(data, R_size);
	}
	
	char *freeslot_pointer;
	//Set the freeslot_pointer value
	freeslot_pointer = data;
	//Set the freeslot_pointer value to appropriate location (according to the size of record)
	freeslot_pointer = freeslot_pointer + (R_id->slot * R_size);
	//Handling of tombstone (we add a star at the end to indicate that it is recently added record)
	*freeslot_pointer = '*';
	//As the page is modified we set the page as dirty
	markDirty(&Record_mgr->bufferPool, &Record_mgr->pageHandle);
	//The data should be then copied to the location pointed by freeslot_pointer
	// This is done using the bulit in function memcpy ( void * destination, const void * source, size_t num )
	memcpy(++freeslot_pointer, record->data + 1, R_size - 1);
	//As the data has been copied we can unpin the page
	unpinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle);
	//As new record has been added the count of tupes has to be incremented
	Record_mgr->tuplesCount++;
	//pinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle, 0);
	return RC_OK;
}

// Deletes the desired record from the table
RC deleteRecord (RM_TableData *rel, RID id)
{
	RM *Record_mgr = rel->mgmtData;
	//We need to pin the page from which we need to delete the record
	pinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle, id.page);
	//Add it to free pages
	Record_mgr->freePage = id.page;

	//Get the data
	char *data;
	data = Record_mgr->pageHandle.data;

	// Calculate the size needed to store the record, as we need to update the pointers accordingly
	int R_size;
	R_size = getRecordSize(rel->schema);
	//Update the data pointer
	data = data + (id.slot * R_size);
	// As we are deleting the record, we add tombstone '#' (indicating deleted record)
	*data = '#';
	// As a record got deleted, we have modified the page, hence need to make it dirty
	markDirty(&Record_mgr->bufferPool, &Record_mgr->pageHandle);
	//As our task has finished we should fre up the space in bufferpool, that is unpin the page
	unpinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle);

	return RC_OK;
}

// Updates the desired record in the table
RC updateRecord (RM_TableData *rel, Record *record)
{	
	RM *Record_mgr = rel->mgmtData;
	//We need to pin the page from which we need to delete the record
	pinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle, record->id.page);
	
	//Assign record id
	RID R_id = record->id; 

	//Get the data
	char *data;
	data = Record_mgr->pageHandle.data;

	// Calculate the location of pointer, as we need to update the pointers accordingly
	int R_size;
	R_size = getRecordSize(rel->schema);
	//Update the data pointer
	data = data + (R_id.slot * R_size);
	
	// Implementing tombstone, as in insert method we had added '*' to indicate it contains some data.
	*data = '*';
	
	//As the page is modified we set the page as dirty
	markDirty(&Record_mgr->bufferPool, &Record_mgr->pageHandle);
	//The data should be then copied to the location pointed by freeslot_pointer
	// This is done using the bulit in function memcpy ( void * destination, const void * source, size_t num )
	memcpy(++data, record->data + 1, R_size - 1);
	//As the data has been copied we can unpin the page
	unpinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle);	
	return RC_OK;	
}

// gets the specified record from the table
RC getRecord (RM_TableData *rel, RID id, Record *record)
{
	RM *Record_mgr = rel->mgmtData;
	//We need to pin the page from which we need to get the record
	pinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle, id.page);

	//Get the data
	char *data;
	data = Record_mgr->pageHandle.data;
	if(*data != '+')
	{
		// Return error if no matching record for Record ID 'id' is found in the table
		return RC_RM_NO_TUPLE_WITH_GIVEN_RID;
	}
	// Calculate the location of pointer, as we need to update the pointers accordingly
	int R_size;
	R_size = getRecordSize(rel->schema);
	//Update the data pointer
	data = data + (id.slot * R_size);
	
	//If we find the record we copy the data of given record
	if(*data == '*')
	{
		//Set record id to the given id
		record->id = id;
		//Pointer to records data
		char *record_data = record->data;
		//The data should be then copied to the location pointed by record_data
		// This is done using the bulit in function memcpy ( void * destination, const void * source, size_t num )
		memcpy(++record_data, data + 1, R_size - 1);	
	}
	else
	{
		//If the record is not found we throw an error
		return RC_NO_RECORD_OF_GIVEN_ID_FOUND;
	}
	//As the data has been copied we can unpin the page
	unpinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle);	
	return RC_OK;
}

// dealing with schemas

// Calculates the size of the record accoring to the schema specified
int getRecordSize (Schema *schema)
{ 	//Record size is initially 0
	int i=0,record_size = 0; 
	//We add the size of each attribute, to calculate the size of entire record
	while(i < schema->numAttr)
	{ 	//We use switch for different data types of the attributes available
		switch(schema->dataTypes[i])
		{	//We add to record_size the size of respective attribute datatype. 
			case DT_INT:
				record_size = record_size + sizeof(int);
				break;
			case DT_STRING:
				//We use typelength here to calculate the size of string
				record_size = record_size + schema->typeLength[i];
				break;
			case DT_FLOAT:
				record_size = record_size + sizeof(float);
				break;
			case DT_BOOL:
				record_size = record_size + sizeof(bool);
				break;
		}
		i=i+1;
	}
	return ++record_size;
}

//Creates a new Schema
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
	//Allocate required memory and set the attributes of schema as specified in the tables.h
	Schema *schema = (Schema *)calloc(1,sizeof(Schema));	
	schema->numAttr = numAttr;
	schema->attrNames = attrNames;
	schema->dataTypes = dataTypes;
	schema->typeLength = typeLength;
	schema->keySize = keySize;
	schema->keyAttrs = keys;
	return schema; 
}

//Frees the schema specified, that is it frees the space occipied by that schema
RC freeSchema (Schema *schema)
{
	//Free the space occipied by the specified schema
	free(schema);
	return RC_OK;
}



//scans

extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
	if(cond!= NULL)
	{
		openTable(rel, "Scan");

		RM *scanManager = (RM*) calloc(1,sizeof(RM));

		scan->mgmtData = scanManager;

		scanManager->recordID.page = 1;

		scanManager->recordID.slot = 0;

		scanManager->scanCount = 0;

		scanManager->condition = cond;

		RM *tableManager = (RM*) calloc(1,sizeof(RM));

		tableManager = rel->mgmtData;

		tableManager->tuplesCount = ATTRIBUTE_SIZE;

		scan->rel = rel;

		return RC_OK;
	}

	//else
	//{
		return RC_NO_RECORD_OF_GIVEN_ID_FOUND;
	//}


}


extern RC next (RM_ScanHandle *scan, Record *record)

{

	RM *scanManager = (RM*) calloc(1,sizeof(RM));

	RM *tableManager = (RM*) calloc(1,sizeof(RM));

	Schema *schema = (Schema *)calloc(1,sizeof(Schema));	


	if(scanManager->condition!= NULL)
	{
		Value *result = (Value*) calloc(1,sizeof(Value));

		int record_size = getRecordSize(schema);

		int totalNoSlots = PAGE_SIZE/record_size;

		int scanCount = scanManager->scanCount;

		int tuplesCount = tableManager->tuplesCount;

		if(tuplesCount == 0)
		{
			return RC_NO_RECORD_OF_GIVEN_ID_FOUND;
		}

		else
		{
			while(scanCount <= tuplesCount)
			{
				if(scanCount <= 0)
				{
					scanManager->recordID.page = 1;
					scanManager->recordID.slot = 0;
				}

				else
				{
					scanManager->recordID.slot++;

					if(scanManager->recordID.slot >= totalNoSlots)
					{
						scanManager->recordID.slot = 0;
						scanManager->recordID.page++;

					}
				}

				pinPage(&tableManager->bufferPool, &scanManager->pageHandle, scanManager->recordID.page);

				char *pageData = scanManager->pageHandle.data;

				pageData = pageData + (scanManager->recordID.slot * record_size);

				record->id.page = scanManager->recordID.page;
				record->id.slot = scanManager->recordID.slot;

				char *dataPointer = record->data;

				*dataPointer = '-';

				memcpy(++dataPointer, pageData+1, record_size-1);

				scanManager->scanCount++;
				scanCount++;

				evalExpr(record, schema, scanManager->condition, &result);

				if(result->v.boolV == TRUE)
				{
					unpinPage(&tableManager->bufferPool, &scanManager->pageHandle);

					return RC_OK; 
				}
			}

			unpinPage(&tableManager->bufferPool, &scanManager->pageHandle);

			scanManager->recordID.page = 1;
			scanManager->recordID.slot = 0;
			scanManager->scanCount = 0;

			return RC_RM_NO_MORE_TUPLES;
		}


	}


	else{

		return RC_NO_RECORD_OF_GIVEN_ID_FOUND;
	}

}


extern RC closeScan (RM_ScanHandle *scan)

{
	RM *scanManager = scan->mgmtData;
	RM *recordManager = scan->rel->mgmtData;

	if(scanManager->scanCount > 0)
	{
		unpinPage(&recordManager->bufferPool, &scanManager->pageHandle);
		
		scanManager->scanCount = 0;
		scanManager->recordID.page = 1;
		scanManager->recordID.slot = 0;
	}

	scan->mgmtData = NULL;
    free(scan->mgmtData);  
	
	return RC_OK;
}


// dealing with records and attribute values

//Creates a new record with the schema specified
RC createRecord (Record **record, Schema *schema)
{
	//Allocating memory of the record
	Record *r_new = (Record*) calloc(1,sizeof(Record));
	// Calculate the size of record according to the schema
	int R_size;
	R_size = getRecordSize(schema);
	//Allocate the space as per calculation  
	r_new->data= (char*) calloc(1,R_size);

	// Setting the records page and id information as -1, we are unaware of this information
	r_new->id.page = -1;
	r_new->id.slot = -1;

	//Set the data pointer to the start of the record's data
	char *data;
	data = r_new->data;
	// As there is no informtion in the record we store '#'. This is done for tombstone implementation.
	*data = '#';
	// We need to add NULL value hence we add \0. 
	*(++data) = '\0';
	//The created record should be substituted by the record specified
	*record = r_new;

	return RC_OK;
}

// //Points to the specific attribute from the initial position of the record. (This function is usefull while we need to get a particular attribute from the schema)
// RC Attribute_pointer (Schema *schema, int attrNum, int *attribute_pos)
// {
// 	int i=0;
// 	//initially the attribute postion will be 1
// 	//*attribute_pos = 1; 
// 	//We add the size of each attribute till we reach the desired attribute (attrNum), so that we pointer to the required attribute
// 	while(i < schema->numAttr)
// 	{ 	//We use switch for different data types of the attributes available
// 		switch(schema->dataTypes[i])
// 		{	//We add to record_size the size of respective attribute datatype. 
// 			case DT_INT:
// 				*attribute_pos = *attribute_pos + sizeof(int);
// 				break;
// 			case DT_STRING:
// 				//We use typelength here to calculate the size of string
// 				*attribute_pos = *attribute_pos + schema->typeLength[i];
// 				break;
// 			case DT_FLOAT:
// 				*attribute_pos = *attribute_pos + sizeof(float);
// 				break;
// 			case DT_BOOL:
// 				*attribute_pos = *attribute_pos + sizeof(bool);
// 				break;
// 		}
// 		i=i+1;
// 	}
// 	return RC_OK;
// }


//Frees the record specified, that is it frees the space occipied by that record
RC freeRecord (Record *record)
{
	//Free the space occipied by the specified schema
	free(record);
	return RC_OK;
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
	int offset = 0;

	// This sets the offset (in bytes) from initial position to the specified attribute of the record into the 'result' parameter passed through the function
	int i;
	offset = 1;

	// Iterating through all the attributes in the schema
	for(i = 0; i < attrNum; i++)
	{
		// Switch depending on DATA TYPE of the ATTRIBUTE
		switch (schema->dataTypes[i])
		{
			// Switch depending on DATA TYPE of the ATTRIBUTE
			case DT_STRING:
				// If attribute is STRING then size = typeLength (Defined Length of STRING)
				offset = offset + schema->typeLength[i];
				break;
			case DT_INT:
				// If attribute is INTEGER, then add size of INT
				offset = offset + sizeof(int);
				break;
			case DT_FLOAT:
				// If attribite is FLOAT, then add size of FLOAT
				offset = offset + sizeof(float);
				break;
			case DT_BOOL:
				// If attribite is BOOLEAN, then add size of BOOLEAN
				offset = offset + sizeof(bool);
				break;
		}
	}
	Value *attr = (Value*) calloc(1, sizeof(Value));

		char *dataPointer;
		dataPointer = record->data + offset;

		schema->dataTypes[attrNum] = (attrNum == 1) ? 1 : schema->dataTypes[attrNum];

		switch(schema->dataTypes[attrNum])
		{

			case DT_INT:
			{

				int val = 0;
				memcpy(&val, dataPointer, sizeof(int));
				attr->v.intV = val;
				attr->dt = DT_INT;
      			break;
			}

			case DT_FLOAT:
			{
				float val;
				memcpy(&val, dataPointer, sizeof(float));
				attr->v.floatV = val;
				attr->dt = DT_FLOAT;
				break;
			}

			case DT_STRING:
			{
				int len = schema->typeLength[attrNum];
				attr->v.stringV = (char*) calloc(1, len + 1);
				strncpy(attr->v.stringV, dataPointer, len);
				attr->v.stringV[len] = '\0';
				attr->dt = DT_STRING;
      			break;

			}

			case DT_BOOL:
			{
				bool val;
				memcpy(&val,dataPointer, sizeof(bool));
				attr->v.boolV = val;
				attr->dt = DT_BOOL;
      			break;
			}

			default:
			printf("\n No such datatype defined");
			break;
		}

		*value = attr;
		return RC_OK;

}


RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
	int offset = 0;

	// This sets the offset (in bytes) from initial position to the specified attribute of the record into the 'result' parameter passed through the function
	int i;
	offset = 1;

	// Iterating through all the attributes in the schema
	for(i = 0; i < attrNum; i++)
	{
		// Switch depending on DATA TYPE of the ATTRIBUTE
		switch (schema->dataTypes[i])
		{
			// Switch depending on DATA TYPE of the ATTRIBUTE
			case DT_STRING:
				// If attribute is STRING then size = typeLength (Defined Length of STRING)
				offset = offset + schema->typeLength[i];
				break;
			case DT_INT:
				// If attribute is INTEGER, then add size of INT
				offset = offset + sizeof(int);
				break;
			case DT_FLOAT:
				// If attribite is FLOAT, then add size of FLOAT
				offset = offset + sizeof(float);
				break;
			case DT_BOOL:
				// If attribite is BOOLEAN, then add size of BOOLEAN
				offset = offset + sizeof(bool);
				break;
		}
	}

	char *dataPointer;
	dataPointer = record->data + offset;

	switch(schema->dataTypes[attrNum])
	{
		case DT_INT:
		{
			*(int *) dataPointer = value->v.intV;	  
			dataPointer = dataPointer + sizeof(int);
		  	break;
		}

		case DT_FLOAT:
		{
			*(float *) dataPointer = value->v.floatV;
			dataPointer = dataPointer + sizeof(float);
			break;
		}

		case DT_STRING:
		{
			int len = schema->typeLength[attrNum];
			strncpy(dataPointer, value->v.stringV, len);
			dataPointer = dataPointer + schema->typeLength[attrNum];
		  	break;
		} 	

	    case DT_BOOL:
		{
			*(bool *) dataPointer = value->v.boolV;
			dataPointer = dataPointer + sizeof(bool);
			break;
		}

		default:
			printf("\n No such datatype defined");
			break;
	}	

	return RC_OK;	

}