// #include <stdio.h>
// #include "storage_mgr.h"
// #include "buffer_mgr.h"
// #include "record_mgr.h"
// #include <stdlib.h>
// #include <sys/stat.h>
// #include <string.h>
// //actual structure of the record manager
// typedef struct RM
// {   
//     //This helps us access the buffer pool insede the record manager
// 	BM_PageHandle pageHandle;	
// 	BM_BufferPool bufferPool;
//     // Stores the record id
// 	RID recordID;
//     //The number of tuples present is stored here
// 	int tuplesCount;
// 	//The conditon used for scanning the records is stored in condition
// 	Expr *condition;
// 	//It tells us where we could find a nearest free slot in the pages
// 	int freePage;
// 	//This gives us the number of scanned records
// 	int scanCount;
// } RM;
// const int MAX_NUMBER_OF_PAGES = 100;
// const int ATTR_SIZE = 15; // Size of the name of the attribute
// RM *Record_mgr;

// //Find a free slot in a page 
// int freeslot_finder(char *data, int record_size)
// {
// 	int i=0;
//     int number_of_slots;
//     number_of_slots = PAGE_SIZE / record_size; 

// 	while(i < number_of_slots)
//     {
// 		if (data[i * record_size] != '+')
// 			return i;
//         else
//         {
//             i++;
//         }
//     }
//     return -1;
// }

// //Initiliaze Record Manager
// RC initRecordManager (void *mgmtData)
// {
// 	//we need to Initiliaze Storage Manager
// 	initStorageManager();
// 	return RC_OK;
// }

// //Shutdown Record Manager
// RC shutdownRecordManager ()
// {
//     //We set the Record_mgr to Null and free the Record_mgr to shut it down
// 	Record_mgr = NULL;
// 	free(Record_mgr);
// 	return RC_OK;
// }

// //This function creates a table qqwith name and schema specified
// RC createTable (char *name, Schema *schema)
// {   
//     SM_FileHandle *fHandle;
//     //Set the data size
//     char data[PAGE_SIZE];
// 	char *pageHandle = data;
// 	//Allocated required memory
// 	Record_mgr = (RM*) calloc(1,sizeof(RM));

// 	//Set buffer pool with max_pages and replacement strategy as LRU
// 	initBufferPool(&Record_mgr->bufferPool, name, MAX_NUMBER_OF_PAGES, RS_LRU, NULL);
//     printf("buffer pool");
//     //0th page for meta data
// 	*(int*)pageHandle = 0; 
// 	pageHandle = pageHandle + sizeof(int);
	
// 	//We set the first page to 1 as the 0th page is used for meta dats storage
// 	*(int*)pageHandle = 1;
// 	pageHandle = pageHandle + sizeof(int);

// 	//Set the number of attributes used (max is set to 15)
// 	*(int*)pageHandle = schema->numAttr;
// 	pageHandle = pageHandle + sizeof(int); 

// 	//After setting attributes, we set its key size
// 	*(int*)pageHandle = schema->keySize;
// 	pageHandle = pageHandle + sizeof(int);
	
//     int i=0;
// 	while(i < schema->numAttr)
//     	{
//         //For all attributes we set its attribute name, data type, and the total size of attribute
// 		//Set attribute name
//        	strncpy(pageHandle, schema->attrNames[i], ATTR_SIZE);
// 	    pageHandle = pageHandle + ATTR_SIZE;
	
// 		//Set data type
// 		*(int*)pageHandle = (int)schema->dataTypes[i];
//        	pageHandle = pageHandle + sizeof(int);

// 		// Set the total size of attribute
// 	   	*(int*)pageHandle = (int) schema->typeLength[i];
// 	   	pageHandle = pageHandle + sizeof(int);
//         i++;
//     	}
		
// 	//Using the storage manager fuctions to create open, write and close file.
//     int return_status;
//     //Create page file and we return the status if any error occurs
//     return_status = createPageFile(name);
// 	if(return_status != RC_OK)
// 		return return_status;
// 	printf("page file created")	;
// 	//Open page file and we return the status if any error occurs
//     return_status = openPageFile(name, fHandle);
// 	if(return_status != RC_OK)
// 		return return_status;
// 	printf("page file opened")	;
// 	//Write page file to the 0th location (we store meta data) and we return the status if any error occurs
//     return_status = writeBlock(0, fHandle, data);
// 	if(return_status != RC_OK)
// 		return return_status;
// 	printf("page file write block")	;	
// 	//Open page file and we return the status if any error occurs
//     return_status = closePageFile(fHandle);
// 	if(return_status != RC_OK)
// 		return return_status;
// 	printf("page file closed")	;
//     //We return RC_OK if everything goes well
// 	return RC_OK;
// }

// //The function opens the table created
// RC openTable (RM_TableData *rel, char *name)
// { 
// 	//Set the name and meta data of table using the relation
// 	rel->name = name;
// 	rel->mgmtData = Record_mgr;
	
// 	//As we need to access the page we need to put it in buffer pull, hence we pin the page
// 	pinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle, 0);
	
//     SM_PageHandle pHandle;   
// 	pHandle = (char*) Record_mgr->pageHandle.data;
	
// 	//Set the tuple count
// 	Record_mgr->tuplesCount = *(int*)pHandle;
// 	pHandle = pHandle + sizeof(int);

// 	//Set the free page for record manager
// 	Record_mgr->freePage= *(int*) pHandle;
//     pHandle = pHandle + sizeof(int);
	
//     //set the number of attributes used
//     int No_of_attributes;
//     No_of_attributes = *(int*)pHandle;
// 	pHandle = pHandle + sizeof(int);


 	
// 	Schema *table_schema;

// 	//Reserving space for table_schema
// 	table_schema = (Schema*)malloc(sizeof(Schema));
    
// 	//Allocating memory and set the variables of table_schema
// 	table_schema->numAttr = No_of_attributes;

// 	table_schema->attrNames = (char**) malloc(sizeof(char*) *No_of_attributes);
//     table_schema->dataTypes = (DataType*) malloc(sizeof(DataType) * No_of_attributes);
// 	table_schema->typeLength = (int*) malloc(sizeof(int) * No_of_attributes);
	
// 	int k=0;
// 	printf("No_of_attributes -> %i\n",No_of_attributes);
// 	while(k < No_of_attributes)
//     {   
// 		//Reserve space for each attributes name
//         table_schema->attrNames[k]= (char*) malloc(No_of_attributes);
//         k++;
//     }
	
	
// 	k=0;  
// 	while(k < table_schema->numAttr)
//     	{
// 		//For all attributes we set its attribute name, data type, and the total size of attribute
// 		strncpy(table_schema->attrNames[k], pHandle, ATTR_SIZE);
// 		pHandle = pHandle + ATTR_SIZE;
	   
// 		table_schema->dataTypes[k]= *(int*) pHandle;
// 		pHandle = pHandle + sizeof(int);

// 		table_schema->typeLength[k]= *(int*)pHandle;
// 		pHandle = pHandle + sizeof(int);
//         k++;
// 	    }
// 	//Set schema of relation as described above
// 	rel->schema = table_schema;	

// 	//As we have finished using the particular page we remove the page from bufferpool hence unpin the page
// 	unpinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle);

		

// 	//wite the particular page back to the disk
// 	forcePage(&Record_mgr->bufferPool, &Record_mgr->pageHandle);
// 	printf("Check here \n");
//     //We return RC_OK if everything goes well
// 	return RC_OK;


// 	// SM_PageHandle pageHandle;    
	
// 	// int attributeCount, k;
	
// 	// // Setting table's meta data to our custom record manager meta data structure
// 	// rel->mgmtData = Record_mgr;
// 	// // Setting the table's name
// 	// rel->name = name;
    
// 	// // Pinning a page i.e. putting a page in Buffer Pool using Buffer Manager
// 	// pinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle, 0);
	
// 	// // Setting the initial pointer (0th location) if the record manager's page data
// 	// pageHandle = (char*) Record_mgr->pageHandle.data;
	
// 	// // Retrieving total number of tuples from the page file
// 	// Record_mgr->tuplesCount= *(int*)pageHandle;
// 	// pageHandle = pageHandle + sizeof(int);

// 	// // Getting free page from the page file
// 	// Record_mgr->freePage= *(int*) pageHandle;
//     // 	pageHandle = pageHandle + sizeof(int);
	
// 	// // Getting the number of attributes from the page file
//     // 	attributeCount = *(int*)pageHandle;
// 	// pageHandle = pageHandle + sizeof(int);
 	
// 	// Schema *schema;

// 	// // Allocating memory space to 'schema'
// 	// schema = (Schema*) malloc(sizeof(Schema));
    
// 	// // Setting schema's parameters
// 	// schema->numAttr = attributeCount;
// 	// schema->attrNames = (char**) malloc(sizeof(char*) *attributeCount);
// 	// schema->dataTypes = (DataType*) malloc(sizeof(DataType) *attributeCount);
// 	// schema->typeLength = (int*) malloc(sizeof(int) *attributeCount);

// 	// // Allocate memory space for storing attribute name for each attribute
// 	// for(k = 0; k < attributeCount; k++)
// 	// 	schema->attrNames[k]= (char*) malloc(ATTR_SIZE);
      
// 	// for(k = 0; k < schema->numAttr; k++)
//     // 	{
// 	// 	// Setting attribute name
// 	// 	strncpy(schema->attrNames[k], pageHandle, ATTR_SIZE);
// 	// 	pageHandle = pageHandle + ATTR_SIZE;
	   
// 	// 	// Setting data type of attribute
// 	// 	schema->dataTypes[k]= *(int*) pageHandle;
// 	// 	pageHandle = pageHandle + sizeof(int);

// 	// 	// Setting length of datatype (length of STRING) of the attribute
// 	// 	schema->typeLength[k]= *(int*)pageHandle;
// 	// 	pageHandle = pageHandle + sizeof(int);
// 	// }
	
// 	// // Setting newly created schema to the table's schema
// 	// rel->schema = schema;	

// 	// // Unpinning the page i.e. removing it from Buffer Pool using BUffer Manager
// 	// unpinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle);

// 	// // Write the page back to disk using BUffer Manger
// 	// forcePage(&Record_mgr->bufferPool, &Record_mgr->pageHandle);

// 	// return RC_OK;
// }   

// // Closes the desired table
// RC closeTable (RM_TableData *rel)
// {
// 	//Perserve the tables meta data, as it would be needed
// 	RM *Record_mgr = rel->mgmtData;
// 	//Free all the resources associated with bufferpool
// 	shutdownBufferPool(&Record_mgr->bufferPool);
// 	return RC_OK;
// }

// //Delete's the desired table
// RC deleteTable (char *name)
// {
// 	//Remove(destroy) that particular page from memory
// 	destroyPageFile(name);
// 	return RC_OK;
// }

// //To get the number of tuples in desired table
// int getNumTuples (RM_TableData *rel)
// {
// 	//As we have stored the tupleCount variable in mgmtData, we access that and return tuplesCount
// 	RM *Record_mgr = rel->mgmtData;
// 	return Record_mgr->tuplesCount;
// }

// // dealing with schemas
// // Calculates the size of the record accoring to the schema specified
// int getRecordSize (Schema *schema)
// { 	//Record size is initially 0
// 	int i=0,record_size = 0; 
// 	//We add the size of each attribute, to calculate the size of entire record
// 	while(i < schema->numAttr)
// 	{ 	//We use switch for different data types of the attributes available
// 		switch(schema->dataTypes[i])
// 		{	//We add to record_size the size of respective attribute datatype. 
// 			case DT_INT:
// 				record_size = record_size + sizeof(int);
// 				break;
// 			case DT_STRING:
// 				//We use typelength here to calculate the size of string
// 				record_size = record_size + schema->typeLength[i];
// 				break;
// 			case DT_FLOAT:
// 				record_size = record_size + sizeof(float);
// 				break;
// 			case DT_BOOL:
// 				record_size = record_size + sizeof(bool);
// 				break;
// 		}
// 		i=i+1;
// 	}
// 	return ++record_size;
// }

// //Creates a new Schema
// Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
// {
// 	//Allocate required memory and set the attributes of schema as specified in the tables.h
// 	Schema *schema = (Schema *)calloc(1,sizeof(Schema));	
// 	schema->numAttr = numAttr;
// 	schema->attrNames = attrNames;
// 	schema->dataTypes = dataTypes;
// 	schema->typeLength = typeLength;
// 	schema->keySize = keySize;
// 	schema->keyAttrs = keys;
// 	return schema; 
// }

// //Frees the schema specified, that is it frees the space occipied by that schema
// RC freeSchema (Schema *schema)
// {
// 	//Free the space occipied by the specified schema
// 	free(schema);
// 	return RC_OK;
// }

// // handling records in a table
// // Insets record into desired table
// RC insertRecord (RM_TableData *rel, Record *record)
// {
// 	RM *Record_mgr = rel->mgmtData;	
// 	//Assign record id
// 	RID *R_id = &record->id; 
// 	char *data;
// 	// Calculate the size needed to store the record
// 	int R_size = getRecordSize(rel->schema);
// 	//Initiaze the page to the free page (to store the record)
// 	R_id->page = Record_mgr->freePage;
// 	//As we are using the current page to store the record, hence the page should be marked pinned. 
// 	pinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle, R_id->page);
// 	data = Record_mgr->pageHandle.data;
	
// 	//Search for the free space using the freeslot_finder function specified
// 	R_id->slot = freeslot_finder(data, R_size);

// 	/*If R_id->slot is equal to -1, then we need to find a free slot in the upcoming pages. For this we need to unpin the page currently in the buffer bool,
// 	add a new page to buffer pool, pin the added page and then check for free slot in this page. We repeat this untill we find a free slot*/
// 	while(R_id->slot == -1)
// 	{
// 		unpinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle);	
// 		R_id->page++;
// 		pinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle, R_id->page);		
// 		data = Record_mgr->pageHandle.data;
// 		R_id->slot = freeslot_finder(data, R_size);
// 	}
	
// 	char *freeslot_pointer;
// 	//Set the freeslot_pointer value
// 	freeslot_pointer = data;
// 	//Set the freeslot_pointer value to appropriate location (according to the size of record)
// 	freeslot_pointer = freeslot_pointer + (R_id->slot * R_size);
// 	//Handling of tombstone (we add a star at the end to indicate that it is recently added record)
// 	*freeslot_pointer = '*';
// 	//As the page is modified we set the page as dirty
// 	markDirty(&Record_mgr->bufferPool, &Record_mgr->pageHandle);
// 	//The data should be then copied to the location pointed by freeslot_pointer
// 	// This is done using the bulit in function memcpy ( void * destination, const void * source, size_t num )
// 	memcpy(++freeslot_pointer, record->data + 1, R_size - 1);
// 	//As the data has been copied we can unpin the page
// 	unpinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle);
// 	//As new record has been added the count of tupes has to be incremented
// 	Record_mgr->tuplesCount++;
// 	//pinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle, 0);
// 	return RC_OK;
// }

// // Deletes the desired record from the table
// RC deleteRecord (RM_TableData *rel, RID id)
// {
// 	RM *Record_mgr = rel->mgmtData;
// 	//We need to pin the page from which we need to delete the record
// 	pinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle, id.page);
// 	//Add it to free pages
// 	Record_mgr->freePage = id.page;

// 	//Get the data
// 	char *data;
// 	data = Record_mgr->pageHandle.data;

// 	// Calculate the size needed to store the record, as we need to update the pointers accordingly
// 	int R_size;
// 	R_size = getRecordSize(rel->schema);
// 	//Update the data pointer
// 	data = data + (id.slot * R_size);
// 	// As we are deleting the record, we add tombstone '#' (indicating deleted record)
// 	*data = '#';
// 	// As a record got deleted, we have modified the page, hence need to make it dirty
// 	markDirty(&Record_mgr->bufferPool, &Record_mgr->pageHandle);
// 	//As our task has finished we should fre up the space in bufferpool, that is unpin the page
// 	unpinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle);

// 	return RC_OK;
// }

// // Updates the desired record in the table
// RC updateRecord (RM_TableData *rel, Record *record)
// {	
// 	RM *Record_mgr = rel->mgmtData;
// 	//We need to pin the page from which we need to delete the record
// 	pinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle, record->id.page);
	
// 	//Assign record id
// 	RID R_id = record->id; 

// 	//Get the data
// 	char *data;
// 	data = Record_mgr->pageHandle.data;

// 	// Calculate the location of pointer, as we need to update the pointers accordingly
// 	int R_size;
// 	R_size = getRecordSize(rel->schema);
// 	//Update the data pointer
// 	data = data + (R_id.slot * R_size);
	
// 	// Implementing tombstone, as in insert method we had added '*' to indicate it contains some data.
// 	*data = '*';
	
// 	//As the page is modified we set the page as dirty
// 	markDirty(&Record_mgr->bufferPool, &Record_mgr->pageHandle);
// 	//The data should be then copied to the location pointed by freeslot_pointer
// 	// This is done using the bulit in function memcpy ( void * destination, const void * source, size_t num )
// 	memcpy(++data, record->data + 1, R_size - 1);
// 	//As the data has been copied we can unpin the page
// 	unpinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle);	
// 	return RC_OK;	
// }

// // gets the specified record from the table
// RC getRecord (RM_TableData *rel, RID id, Record *record)
// {
// 	RM *Record_mgr = rel->mgmtData;
// 	//We need to pin the page from which we need to get the record
// 	pinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle, id.page);

// 	//Get the data
// 	char *data;
// 	data = Record_mgr->pageHandle.data;

// 	// Calculate the location of pointer, as we need to update the pointers accordingly
// 	int R_size;
// 	R_size = getRecordSize(rel->schema);
// 	//Update the data pointer
// 	data = data + (id.slot * R_size);
	
// 	//If we find the record we copy the data of given record
// 	if(*data = '*')
// 	{
// 		//Set record id to the given id
// 		record->id = id;
// 		//Pointer to records data
// 		char *record_data = record->data;
// 		//The data should be then copied to the location pointed by record_data
// 		// This is done using the bulit in function memcpy ( void * destination, const void * source, size_t num )
// 		memcpy(++record_data, data + 1, R_size - 1);	
// 	}
// 	else
// 	{
// 		//If the record is not found we throw an error
// 		return RC_NO_RECORD_OF_GIVEN_ID_FOUND;
// 	}
// 	//As the data has been copied we can unpin the page
// 	unpinPage(&Record_mgr->bufferPool, &Record_mgr->pageHandle);	
// 	return RC_OK;
// }

// //scans
// RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
// {
// 	if(cond!= NULL)
// 	{
// 		openTable(rel, "Scan");

// 		RM *scanManager = (RM*) calloc(1,sizeof(RM));

// 		scan->mgmtData = scanManager;

// 		scanManager->recordID.page = 1;

// 		scanManager->recordID.slot = 0;

// 		scanManager->scanCount = 0;

// 		scanManager->condition = cond;

// 		RM *tableManager = (RM*) calloc(1,sizeof(RM));

// 		tableManager = rel->mgmtData;

// 		tableManager->tuplesCount = ATTR_SIZE;

// 		scan->rel = rel;

// 		return RC_OK;
// 	}

// 	else
// 	{
// 		return RC_SCAN_CONDITION_NOT_FOUND;
// 	}


// }


// RC next (RM_ScanHandle *scan, Record *record)

// {

// 	RM *scanManager = (RM*) calloc(1,sizeof(RM));

// 	RM *tableManager = (RM*) calloc(1,sizeof(RM));

// 	Schema *schema = (Schema *)calloc(1,sizeof(Schema));	


// 	if(scanManager->condition!= NULL)
// 	{
// 		Value *attribute_pos = (Value*) calloc(1,sizeof(Value));

// 		int record_size = getRecordSize(schema);

// 		int totalNoSlots = PAGE_SIZE/record_size;

// 		int scanCount = scanManager->scanCount;

// 		int tuplesCount = tableManager->tuplesCount;

// 		if(tuplesCount == 0)
// 		{
// 			return RC_RM_NO_MORE_TUPLES;
// 		}

// 		else
// 		{
// 			while(scanCount <= tuplesCount)
// 			{
// 				if(scanCount <= 0)
// 				{
// 					scanManager->recordID.page = 1;
// 					scanManager->recordID.slot = 0;
// 				}

// 				else
// 				{
// 					scanManager->recordID.slot++;

// 					if(scanManager->recordID.slot >= totalNoSlots)
// 					{
// 						scanManager->recordID.slot = 0;
// 						scanManager->recordID.page++;

// 					}
// 				}

// 				pinPage(&tableManager->bufferPool, &scanManager->pageHandle, scanManager->recordID.page);

// 				char *pageData = scanManager->pageHandle.data;

// 				pageData = pageData + (scanManager->recordID.slot * record_size);

// 				record->id.page = scanManager->recordID.page;
// 				record->id.slot = scanManager->recordID.slot;

// 				char *dataPointer = record->data;

// 				*dataPointer = '#';

// 				memcpy(++dataPointer, pageData+1, record_size-1);

// 				scanManager->scanCount++;
// 				scanCount++;

// 				evalExpr(record, schema, scanManager->condition, &attribute_pos);

// 				if(attribute_pos->v.boolV == TRUE)
// 				{
// 					unpinPage(&tableManager->bufferPool, &scanManager->pageHandle);

// 					return RC_OK; 
// 				}
// 			}

// 			unpinPage(&tableManager->bufferPool, &scanManager->pageHandle);

// 			scanManager->recordID.page = 1;
// 			scanManager->recordID.slot = 0;
// 			scanManager->scanCount = 0;

// 			return RC_RM_NO_MORE_TUPLES;
// 		}


// 	}


// 	else{

// 		return RC_SCAN_CONDITION_NOT_FOUND;
// 	}

// }


// RC closeScan (RM_ScanHandle *scan)

// {
// 	RM *scanManager = scan->mgmtData;
// 	RM *rec_mgr = scan->rel->mgmtData;

// 	if(scanManager->scanCount > 0)
// 	{
// 		unpinPage(&rec_mgr->bufferPool, &scanManager->pageHandle);
		
// 		scanManager->scanCount = 0;
// 		scanManager->recordID.page = 1;
// 		scanManager->recordID.slot = 0;
// 	}

// 	scan->mgmtData = NULL;
//     free(scan->mgmtData);  
	
// 	return RC_OK;
// }



// // dealing with records and attribute values
// //Creates a new record with the schema specified
// RC createRecord (Record **record, Schema *schema)
// {
// 	//Allocating memory of the record
// 	Record *r_new = (Record*) calloc(1,sizeof(Record));
// 	// Calculate the size of record according to the schema
// 	int R_size;
// 	R_size = getRecordSize(schema);
// 	//Allocate the space as per calculation  
// 	r_new->data= (char*) calloc(1,R_size);

// 	// Setting the records page and id information as -1, we are unaware of this information
// 	r_new->id.page = -1;
// 	r_new->id.slot = -1;

// 	//Set the data pointer to the start of the record's data
// 	char *data;
// 	data = r_new->data;
// 	// As there is no informtion in the record we store '#'. This is done for tombstone implementation.
// 	*data = '#';
// 	// We need to add NULL value hence we add \0. 
// 	*(++data) = '\0';
// 	//The created record should be substituted by the record specified
// 	*record = r_new;

// 	return RC_OK;
// }

// // //Points to the specific attribute from the initial position of the record. (This function is usefull while we need to get a particular attribute from the schema)
// // RC Attribute_pointer (Schema *schema, int attrNum, int *attribute_pos)
// // {
// // 	int i=0,
// // 	//initially the attribute postion will be 1
// // 	*attribute_pos = 1; 
// // 	//We add the size of each attribute till we reach the desired attribute (attrNum), so that we pointer to the required attribute
// // 	while(i < attrNum)
// // 	{ 	//We use switch for different data types of the attributes available
// // 		switch(schema->dataTypes[i])
// // 		{	//We add to record_size the size of respective attribute datatype. 
// // 			case DT_INT:
// // 				*attribute_pos = *attribute_pos + sizeof(int);
// // 				break;
// // 			case DT_STRING:
// // 				//We use typelength here to calculate the size of string
// // 				*attribute_pos = *attribute_pos + schema->typeLength[i];
// // 				break;
// // 			case DT_FLOAT:
// // 				*attribute_pos = *attribute_pos + sizeof(float);
// // 				break;
// // 			case DT_BOOL:
// // 				*attribute_pos = *attribute_pos + sizeof(bool);
// // 				break;
// // 		}
// // 		i=i+1;
// // 	}
// // 	return RC_OK;
// // }
// //Frees the record specified, that is it frees the space occipied by that record
// RC freeRecord (Record *record)
// {
// 	//Free the space occipied by the specified schema
// 	free(record);
// 	return RC_OK;
// }

// RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
// {	
// 	///Points to the specific attribute from the initial position of the record.
// 	int position_attr = 0;
// 	int i = 0;
// 	////initially the attribute postion will be 1
// 	position_attr = 1;

// 	//We add the size of each attribute till we reach the desired attribute (attrNum), so that we pointer to the required attribute
// 	while(i < attrNum)
// 	{
// 		//We use switch for different data types of the attributes available
// 		switch (schema->dataTypes[i])
// 		{
// 			case DT_INT:
// 				position_attr = position_attr + sizeof(int);
// 				break;
// 			//We use typelength here to calculate the size of string 
// 			case DT_STRING:
// 				position_attr = position_attr + schema->typeLength[i];
// 				break;
// 			case DT_FLOAT:
// 				position_attr = position_attr + sizeof(float);
// 				break;
// 			case DT_BOOL:
// 				position_attr = position_attr + sizeof(bool);
// 				break;
// 		}
// 		i=i+1;
// 	}


// 	Value *attr = (Value*) calloc(1, sizeof(Value));

// 		char *dataPointer;
// 		dataPointer = record->data + position_attr;

// 		schema->dataTypes[attrNum] = (attrNum == 1) ? 1 : schema->dataTypes[attrNum];

// 		switch(schema->dataTypes[attrNum])
// 		{

// 			case DT_INT:
// 			{

// 				int val = 0;
// 				memcpy(&val, dataPointer, sizeof(int));
// 				attr->v.intV = val;
// 				attr->dt = DT_INT;
//       			break;
// 			}

// 			case DT_FLOAT:
// 			{
// 				float val;
// 				memcpy(&val, dataPointer, sizeof(float));
// 				attr->v.floatV = val;
// 				attr->dt = DT_FLOAT;
// 				break;
// 			}

// 			case DT_STRING:
// 			{
// 				int len = schema->typeLength[attrNum];
// 				attr->v.stringV = (char*) calloc(1, len + 1);
// 				strncpy(attr->v.stringV, dataPointer, len);
// 				attr->v.stringV[len] = '\0';
// 				attr->dt = DT_STRING;
//       			break;

// 			}

// 			case DT_BOOL:
// 			{
// 				bool val;
// 				memcpy(&val,dataPointer, sizeof(bool));
// 				attr->v.boolV = val;
// 				attr->dt = DT_BOOL;
//       			break;
// 			}

// 			default:
// 			printf("\n No such datatype defined");
// 			break;
// 		}

// 		*value = attr;
// 		return RC_OK;

// }


// RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
// {
// 	int offset = 0;
// 	int i;
// 	offset = 1;

// 	// Iterating through all the attributes in the schema
// 	for(i = 0; i < attrNum; i++)
// 	{
// 		// Switch depending on DATA TYPE of the ATTRIBUTE
// 		switch (schema->dataTypes[i])
// 		{
// 			// Switch depending on DATA TYPE of the ATTRIBUTE
// 			case DT_STRING:
// 				// If attribute is STRING then size = typeLength (Defined Length of STRING)
// 				offset = offset + schema->typeLength[i];
// 				break;
// 			case DT_INT:
// 				// If attribute is INTEGER, then add size of INT
// 				offset = offset + sizeof(int);
// 				break;
// 			case DT_FLOAT:
// 				// If attribite is FLOAT, then add size of FLOAT
// 				offset = offset + sizeof(float);
// 				break;
// 			case DT_BOOL:
// 				// If attribite is BOOLEAN, then add size of BOOLEAN
// 				offset = offset + sizeof(bool);
// 				break;
// 		}
// 	}

// 	char *dataPointer;
// 	dataPointer = record->data + offset;

// 	switch(schema->dataTypes[attrNum])
// 	{
// 		case DT_INT:
// 		{
// 			*(int *) dataPointer = value->v.intV;	  
// 			dataPointer = dataPointer + sizeof(int);
// 		  	break;
// 		}

// 		case DT_FLOAT:
// 		{
// 			*(float *) dataPointer = value->v.floatV;
// 			dataPointer = dataPointer + sizeof(float);
// 			break;
// 		}

// 		case DT_STRING:
// 		{
// 			int len = schema->typeLength[attrNum];
// 			strncpy(dataPointer, value->v.stringV, len);
// 			dataPointer = dataPointer + schema->typeLength[attrNum];
// 		  	break;
// 		} 	

// 	    case DT_BOOL:
// 		{
// 			*(bool *) dataPointer = value->v.boolV;
// 			dataPointer = dataPointer + sizeof(bool);
// 			break;
// 		}

// 		default:
// 			printf("\n No such datatype defined");
// 			break;
// 	}	

// 	return RC_OK;	

// }



#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

// This is custom data structure defined for making the use of Record Manager.
typedef struct RM
{
	// Buffer Manager's PageHandle for using Buffer Manager to access Page files
	BM_PageHandle pageHandle;	// Buffer Manager PageHandle 
	// Buffer Manager's Buffer Pool for using Buffer Manager	
	BM_BufferPool bufferPool;
	// Record ID	
	RID recordID;
	// This variable defines the condition for scanning the records in the table
	Expr *condition;
	// This variable stores the total number of tuples in the table
	int tuplesCount;
	// This variable stores the location of first free page which has empty slots in table
	int freePage;
	// This variable stores the count of the number of records scanned
	int scanCount;
} RM;

const int NO_pages_max = 100;
const int ATTR_SIZE = 15; // Size of the name of the attribute

RM *rec_mgr;

// ******** CUSTOM FUNCTIONS ******** //

// This function returns a free slot within a page
int findFreeSlot(char *data, int recordSize)
{
	int i, totalSlots = PAGE_SIZE / recordSize; 

	for (i = 0; i < totalSlots; i++)
		if (data[i * recordSize] != '+')
			return i;
	return -1;
}


// ******** TABLE AND RECORD MANAGER FUNCTIONS ******** //

// This function initializes the Record Manager
extern RC initRecordManager (void *mgmtData)
{
	// Initiliazing Storage Manager
	initStorageManager();
	return RC_OK;
}

// This functions shuts down the Record Manager
extern RC shutdownRecordManager ()
{
	rec_mgr = NULL;
	free(rec_mgr);
	return RC_OK;
}

// This function creates a TABLE with table name "name" having schema specified by "schema"
extern RC createTable (char *name, Schema *schema)
{
	// Allocating memory space to the record manager custom data structure
	rec_mgr = (RM*) malloc(sizeof(RM));

	// Initalizing the Buffer Pool using LFU page replacement policy
	initBufferPool(&rec_mgr->bufferPool, name, NO_pages_max, RS_LRU, NULL);

	char data[PAGE_SIZE];
	char *pHandle = data;
	 
	int res, k;

	// Setting number of tuples to 0
	*(int*)pHandle = 0; 

	// Incrementing pointer by sizeof(int) because 0 is an integer
	pHandle = pHandle + sizeof(int);
	
	// Setting first page to 1 since 0th page if for schema and other meta data
	*(int*)pHandle = 1;

	// Incrementing pointer by sizeof(int) because 1 is an integer
	pHandle = pHandle + sizeof(int);

	// Setting the number of attributes
	*(int*)pHandle = schema->numAttr;

	// Incrementing pointer by sizeof(int) because number of attributes is an integer
	pHandle = pHandle + sizeof(int); 

	// Setting the Key Size of the attributes
	*(int*)pHandle = schema->keySize;

	// Incrementing pointer by sizeof(int) because Key Size of attributes is an integer
	pHandle = pHandle + sizeof(int);
	
	k = 0;
	while(k < schema->numAttr)
    	{
		// Setting attribute name
       		strncpy(pHandle, schema->attrNames[k], ATTR_SIZE);
	       	pHandle = pHandle + ATTR_SIZE;
	
		// Setting data type of attribute
	       	*(int*)pHandle = (int)schema->dataTypes[k];

		// Incrementing pointer by sizeof(int) because we have data type using integer constants
	       	pHandle = pHandle + sizeof(int);

		// Setting length of datatype of the attribute
	       	*(int*)pHandle = (int) schema->typeLength[k];

		// Incrementing pointer by sizeof(int) because type length is an integer
	       	pHandle = pHandle + sizeof(int);
		
		k++;
    	}

	SM_FileHandle fileHandle;
		
	// Creating a page file page name as table name using storage manager
	if((res = createPageFile(name)) != RC_OK)
		return res;
		
	// Opening the newly created page
	if((res = openPageFile(name, &fileHandle)) != RC_OK)
		return res;
		
	// Writing the schema to first location of the page file
	if((res = writeBlock(0, &fileHandle, data)) != RC_OK)
		return res;
		
	// Closing the file after writing
	if((res = closePageFile(&fileHandle)) != RC_OK)
		return res;

	return RC_OK;
}

// This function opens the table with table name "name"
extern RC openTable (RM_TableData *rel, char *name)
{
	SM_PageHandle pHandle;    
	
	int attributeCount, k;
	
	// Setting table's meta data to our custom record manager meta data structure
	rel->mgmtData = rec_mgr;
	// Setting the table's name
	rel->name = name;
    
	// Pinning a page i.e. putting a page in Buffer Pool using Buffer Manager
	pinPage(&rec_mgr->bufferPool, &rec_mgr->pageHandle, 0);
	
	// Setting the initial pointer (0th location) if the record manager's page data
	pHandle = (char*) rec_mgr->pageHandle.data;
	
	// Retrieving total number of tuples from the page file
	rec_mgr->tuplesCount= *(int*)pHandle;
	pHandle = pHandle + sizeof(int);

	// Getting free page from the page file
	rec_mgr->freePage= *(int*) pHandle;
    	pHandle = pHandle + sizeof(int);
	
	// Getting the number of attributes from the page file
    	attributeCount = *(int*)pHandle;
	pHandle = pHandle + sizeof(int);
 	
	Schema *table_schema;

	// Allocating memory space to 'schema'
	table_schema = (Schema*) malloc(sizeof(Schema));
    
	// Setting schema's parameters
	table_schema->numAttr = attributeCount;
	table_schema->attrNames = (char**) malloc(sizeof(char*) *attributeCount);
	table_schema->dataTypes = (DataType*) malloc(sizeof(DataType) *attributeCount);
	table_schema->typeLength = (int*) malloc(sizeof(int) *attributeCount);

	// Allocate memory space for storing attribute name for each attribute
	for(k = 0; k < attributeCount; k++){
		table_schema->attrNames[k]= (char*) malloc(ATTR_SIZE);
	}
	k = 0;
	while(k < table_schema->numAttr)
    	{
		// Setting attribute name
		strncpy(table_schema->attrNames[k], pHandle, ATTR_SIZE);
		pHandle = pHandle + ATTR_SIZE;
	   
		// Setting data type of attribute
		table_schema->dataTypes[k]= *(int*) pHandle;
		pHandle = pHandle + sizeof(int);

		// Setting length of datatype (length of STRING) of the attribute
		table_schema->typeLength[k]= *(int*)pHandle;
		pHandle = pHandle + sizeof(int);
		k++;
	}
	
	// Setting newly created schema to the table's schema
	rel->schema = table_schema;	

	// Unpinning the page i.e. removing it from Buffer Pool using BUffer Manager
	unpinPage(&rec_mgr->bufferPool, &rec_mgr->pageHandle);

	// Write the page back to disk using BUffer Manger
	forcePage(&rec_mgr->bufferPool, &rec_mgr->pageHandle);

	return RC_OK;
}   
  
// This function closes the table referenced by "rel"
extern RC closeTable (RM_TableData *rel)
{
	// Storing the Table's meta data
	RM *rec_mgr = rel->mgmtData;
	
	// Shutting down Buffer Pool	
	shutdownBufferPool(&rec_mgr->bufferPool);
	//rel->mgmtData = NULL;
	return RC_OK;
}

// This function deletes the table having table name "name"
extern RC deleteTable (char *name)
{
	// Removing the page file from memory using storage manager
	destroyPageFile(name);
	return RC_OK;
}

// This function returns the number of tuples (records) in the table referenced by "rel"
extern int getNumTuples (RM_TableData *rel)
{
	// Accessing our data structure's tuplesCount and returning it
	RM *rec_mgr = rel->mgmtData;
	return rec_mgr->tuplesCount;
}


// ******** RECORD FUNCTIONS ******** //

// This function inserts a new record in the table referenced by "rel" and updates the 'record' parameter with the Record ID of he newly inserted record
extern RC insertRecord (RM_TableData *rel, Record *record)
{
	// Retrieving our meta data stored in the table
	RM *rec_mgr = rel->mgmtData;	
	
	// Setting the Record ID for this record
	RID *recId = &record->id; 
	
	char *data, *slot;
	
	// Getting the size in bytes needed to store on record for the given schema
	int recordSize = getRecordSize(rel->schema);
	
	// Setting first free page to the current page
	recId->page = rec_mgr->freePage;

	// Pinning page i.e. telling Buffer Manager that we are using this page
	pinPage(&rec_mgr->bufferPool, &rec_mgr->pageHandle, recId->page);
	
	// Setting the data to initial position of record's data
	data = rec_mgr->pageHandle.data;
	
	// Getting a free slot using our custom function
	recId->slot = findFreeSlot(data, recordSize);

	while(recId->slot == -1)
	{
		// If the pinned page doesn't have a free slot then unpin that page
		unpinPage(&rec_mgr->bufferPool, &rec_mgr->pageHandle);	
		
		// Incrementing page
		recId->page++;
		
		// Bring the new page into the BUffer Pool using Buffer Manager
		pinPage(&rec_mgr->bufferPool, &rec_mgr->pageHandle, recId->page);
		
		// Setting the data to initial position of record's data		
		data = rec_mgr->pageHandle.data;

		// Again checking for a free slot using our custom function
		recId->slot = findFreeSlot(data, recordSize);
	}
	
	slot = data;
	
	// Mark page dirty to notify that this page was modified
	markDirty(&rec_mgr->bufferPool, &rec_mgr->pageHandle);
	
	// Calculation slot starting position
	slot = slot + (recId->slot * recordSize);

	// Appending '+' as tombstone to indicate this is a new record and should be removed if space is lesss
	*slot = '+';

	// Copy the record's data to the memory location pointed by slotPointer
	memcpy(++slot, record->data + 1, recordSize - 1);

	// Unpinning a page i.e. removing a page from the BUffer Pool
	unpinPage(&rec_mgr->bufferPool, &rec_mgr->pageHandle);
	
	// Incrementing count of tuples
	rec_mgr->tuplesCount++;
	
	// Pinback the page	
	pinPage(&rec_mgr->bufferPool, &rec_mgr->pageHandle, 0);

	return RC_OK;
}

// This function deletes a record having Record ID "id" in the table referenced by "rel"
extern RC deleteRecord (RM_TableData *rel, RID id)
{
	// Retrieving our meta data stored in the table
	RM *rec_mgr = rel->mgmtData;
	
	// Pinning the page which has the record which we want to update
	pinPage(&rec_mgr->bufferPool, &rec_mgr->pageHandle, id.page);

	// Update free page because this page 
	rec_mgr->freePage = id.page;
	
	char *rec_data = rec_mgr->pageHandle.data;

	// Getting the size of the record
	int recordSize = getRecordSize(rel->schema);

	// Setting data pointer to the specific slot of the record
	rec_data = rec_data + (id.slot * recordSize);
	
	// '-' is used for Tombstone mechanism. It denotes that the record is deleted
	*rec_data = '-';
		
	// Mark the page dirty because it has been modified
	markDirty(&rec_mgr->bufferPool, &rec_mgr->pageHandle);

	// Unpin the page after the record is retrieved since the page is no longer required to be in memory
	unpinPage(&rec_mgr->bufferPool, &rec_mgr->pageHandle);

	return RC_OK;
}

// This function updates a record referenced by "record" in the table referenced by "rel"
extern RC updateRecord (RM_TableData *rel, Record *record)
{	
	// Retrieving our meta data stored in the table
	RM *rec_mgr = rel->mgmtData;
	
	// Pinning the page which has the record which we want to update
	pinPage(&rec_mgr->bufferPool, &rec_mgr->pageHandle, record->id.page);

	char *rec_data;

	// Getting the size of the record
	int recordSize = getRecordSize(rel->schema);

	// Set the Record's ID
	RID id = record->id;

	// Getting record data's memory location and calculating the start position of the new data
	rec_data = rec_mgr->pageHandle.data;
	rec_data = rec_data + (id.slot * recordSize);
	
	// '+' is used for Tombstone mechanism. It denotes that the record is not empty
	*rec_data = '+';
	
	// Copy the new record data to the exisitng record
	memcpy(++rec_data, record->data + 1, recordSize - 1 );
	
	// Mark the page dirty because it has been modified
	markDirty(&rec_mgr->bufferPool, &rec_mgr->pageHandle);

	// Unpin the page after the record is retrieved since the page is no longer required to be in memory
	unpinPage(&rec_mgr->bufferPool, &rec_mgr->pageHandle);
	
	return RC_OK;	
}

// This function retrieves a record having Record ID "id" in the table referenced by "rel".
// The result record is stored in the location referenced by "record"
extern RC getRecord (RM_TableData *rel, RID id, Record *record)
{
	// Retrieving our meta data stored in the table
	RM *rec_mgr = rel->mgmtData;
	
	// Pinning the page which has the record we want to retreive
	pinPage(&rec_mgr->bufferPool, &rec_mgr->pageHandle, id.page);

	// Getting the size of the record
	int size_rec = getRecordSize(rel->schema);
	char *pointer_to_data = rec_mgr->pageHandle.data;
	pointer_to_data = pointer_to_data + (id.slot * size_rec);
	
	if(*pointer_to_data != '+')
	{
		// Return error if no matching record for Record ID 'id' is found in the table
		return RC_RM_NO_TUPLE_WITH_GIVEN_RID;
	}
	else
	{
		// Setting the Record ID
		record->id = id;

		// Setting the pointer to data field of 'record' so that we can copy the data of the record
		char *rec_data = record->data;

		// Copy data using C's function memcpy(...)
		memcpy(++rec_data, pointer_to_data + 1, size_rec - 1);
	}

	// Unpin the page after the record is retrieved since the page is no longer required to be in memory
	unpinPage(&rec_mgr->bufferPool, &rec_mgr->pageHandle);

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

    RM *scan_mgr;
	RM *table_mgr;

	// Allocating some memory to the scanManager
    scan_mgr = (RM*) malloc(sizeof(RM));
    	
	// Setting the scan's meta data to our meta data
    scan->mgmtData = scan_mgr;
    	
	// 1 to start scan from the first page
    scan_mgr->recordID.page = 1;
    	
	// 0 to start scan from the first slot	
	scan_mgr->recordID.slot = 0;
	
	// 0 because this just initializing the scan. No records have been scanned yet    	
	scan_mgr->scanCount = 0;

	// Setting the scan condition
    scan_mgr->condition = cond;
    	
	// Setting the our meta data to the table's meta data
    table_mgr = rel->mgmtData;

	// Setting the tuple count
    table_mgr->tuplesCount = ATTR_SIZE;

	// Setting the scan's table i.e. the table which has to be scanned using the specified condition
    scan->rel= rel;

	return RC_OK;
}

// This function scans each record in the table and stores the result record (record satisfying the condition)
// in the location pointed by  'record'.
extern RC next (RM_ScanHandle *scan, Record *record)
{
	// Initiliazing scan data
	RM *scan_mgr = scan->mgmtData;
	RM *table_mgr = scan->rel->mgmtData;
    Schema *schema = scan->rel->schema;
	
	// Checking if scan condition (test expression) is present
	if (scan_mgr->condition == NULL)
	{
		return RC_SCAN_CONDITION_NOT_FOUND;
	}

	Value *res = (Value *) malloc(sizeof(Value));
   
	char *data;
   	
	// Getting record size of the schema
	int rec_size = getRecordSize(schema);

	// Calculating Total number of slots
	int tot_slots = PAGE_SIZE / rec_size;

	// Getting Scan Count
	int no_of_scans = scan_mgr->scanCount;

	// Getting tuples count of the table
	int tuples_count = table_mgr->tuplesCount;

	// Checking if the table contains tuples. If the tables doesn't have tuple, then return respective message code
	if (tuples_count == 0)
		return RC_RM_NO_MORE_TUPLES;

	// Iterate through the tuples
	while(no_of_scans <= tuples_count)
	{  
		// If all the tuples have been scanned, execute this block
		if (no_of_scans <= 0)
		{
			// printf("INSIDE If scanCount <= 0 \n");
			// Set PAGE and SLOT to first position
			scan_mgr->recordID.page = 1;
			scan_mgr->recordID.slot = 0;
		}
		else
		{
			// printf("INSIDE Else scanCount <= 0 \n");
			scan_mgr->recordID.slot++;

			// If all the slots have been scanned execute this block
			if(scan_mgr->recordID.slot >= tot_slots)
			{
				scan_mgr->recordID.slot = 0;
				scan_mgr->recordID.page++;
			}
		}

		// Pinning the page i.e. putting the page in buffer pool
		pinPage(&table_mgr->bufferPool, &scan_mgr->pageHandle, scan_mgr->recordID.page);
			
		// Retrieving the data of the page			
		data = scan_mgr->pageHandle.data;

		// Calulate the data location from record's slot and record size
		data = data + (scan_mgr->recordID.slot * rec_size);
		
		// Set the record's slot and page to scan manager's slot and page
		record->id.page = scan_mgr->recordID.page;
		record->id.slot = scan_mgr->recordID.slot;

		// Intialize the record data's first location
		char *dataPointer = record->data;

		// '-' is used for Tombstone mechanism.
		*dataPointer = '-';
		
		memcpy(++dataPointer, data + 1, rec_size - 1);

		// Increment scan count because we have scanned one record
		scan_mgr->scanCount++;
		no_of_scans++;

		// Test the record for the specified condition (test expression)
		evalExpr(record, schema, scan_mgr->condition, &res); 

		// v.boolV is TRUE if the record satisfies the condition
		if(res->v.boolV == TRUE)
		{
			// Unpin the page i.e. remove it from the buffer pool.
			unpinPage(&table_mgr->bufferPool, &scan_mgr->pageHandle);
			// Return SUCCESS			
			return RC_OK;
		}
	}
	
	// Unpin the page i.e. remove it from the buffer pool.
	unpinPage(&table_mgr->bufferPool, &scan_mgr->pageHandle);
	
	// Reset the Scan Manager's values
	scan_mgr->recordID.page = 1;
	scan_mgr->recordID.slot = 0;
	scan_mgr->scanCount = 0;
	
	// None of the tuple satisfy the condition and there are no more tuples to scan
	return RC_RM_NO_MORE_TUPLES;
}

// This function closes the scan operation.
extern RC closeScan (RM_ScanHandle *scan)
{
	RM *scan_mgr = scan->mgmtData;
	RM *rec_mgr = scan->rel->mgmtData;

	// Check if scan was incomplete
	if(scan_mgr->scanCount > 0)
	{
		// Unpin the page i.e. remove it from the buffer pool.
		unpinPage(&rec_mgr->bufferPool, &scan_mgr->pageHandle);
		
		// Reset the Scan Manager's values
		scan_mgr->scanCount = 0;
		scan_mgr->recordID.page = 1;
		scan_mgr->recordID.slot = 0;
	}
	
	// De-allocate all the memory space allocated to the scans's meta data (our custom structure)
    	scan->mgmtData = NULL;
    	free(scan->mgmtData);  
	
	return RC_OK;
}


// ******** SCHEMA FUNCTIONS ******** //

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

// This function removes a schema from memory and de-allocates all the memory space allocated to the schema.
extern RC freeSchema (Schema *schema)
{
	// De-allocating memory space occupied by 'schema'
	free(schema);
	return RC_OK;
}


// ******** DEALING WITH RECORDS AND ATTRIBUTE VALUES ******** //

// This function creates a new record in the schema referenced by "schema"
extern RC createRecord (Record **record, Schema *schema)
{
	// Allocate some memory space for the new record
	Record *rec_new = (Record*) malloc(sizeof(Record));
	
	// Retrieve the record size
	int recordSize = getRecordSize(schema);

	// Allocate some memory space for the data of new record    
	rec_new->data= (char*) malloc(recordSize);

	// Setting page and slot position. -1 because this is a new record and we don't know anything about the position
	rec_new->id.page = rec_new->id.slot = -1;

	// Getting the starting position in memory of the record's data
	char *dataPointer = rec_new->data;
	
	// '-' is used for Tombstone mechanism. We set it to '-' because the record is empty.
	*dataPointer = '-';
	
	// Append '\0' which means NULL in C to the record after tombstone. ++ because we need to move the position by one before adding NULL
	*(++dataPointer) = '\0';

	// Set the newly created record to 'record' which passed as argument
	*record = rec_new;

	return RC_OK;
}

// This function sets the offset (in bytes) from initial position to the specified attribute of the record into the 'result' parameter passed through the function
RC attrOffset (Schema *schema, int attrNum, int *result)
{
	int i;
	*result = 1;

	i = 0;
	// Iterating through all the attributes in the schema
	while(i < attrNum)
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
		i++;
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
	Value *attr = (Value*) malloc(sizeof(Value));

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
			int len = schema->typeLength[attrNum];
			// Allocate space for string hving size - 'len'
			attr->v.stringV = (char *) malloc(len + 1);

			// Copying string to location pointed by dataPointer and appending '\0' which denotes end of string in C
			strncpy(attr->v.stringV, dataPointer, len);
			attr->v.stringV[len] = '\0';
			attr->dt = DT_STRING;
      			break;
		}

		case DT_INT:
		{
			// Getting attribute value from an attribute of type INTEGER
			int val = 0;
			memcpy(&val, dataPointer, sizeof(int));
			attr->v.intV = val;
			attr->dt = DT_INT;
      			break;
		}
    
		case DT_FLOAT:
		{
			// Getting attribute val from an attribute of type FLOAT
	  		float val;
	  		memcpy(&val, dataPointer, sizeof(float));
	  		attr->v.floatV = val;
			attr->dt = DT_FLOAT;
			break;
		}

		case DT_BOOL:
		{
			// Getting attribute val from an attribute of type BOOLEAN
			bool val;
			memcpy(&val,dataPointer, sizeof(bool));
			attr->v.boolV = val;
			attr->dt = DT_BOOL;
      			break;
		}

		default:
			printf("Serializer not defined for the given datatype. \n");
			break;
	}

	*value = attr;
	return RC_OK;
}

// This function sets the attribute value in the record in the specified schema
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
	int offset = 0;

	// Getting the ofset value of attributes depending on the attribute number
	attrOffset(schema, attrNum, &offset);

	// Getting the starting position of record's data in memory
	char *dataPtr = record->data;
	
	// Adding offset to the starting position
	dataPtr = dataPtr + offset;
		
	switch(schema->dataTypes[attrNum])
	{
		case DT_STRING:
		{
			// Setting attribute value of an attribute of type STRING
			// Getting the legeth of the string as defined while creating the schema
			int length = schema->typeLength[attrNum];

			// Copying attribute's value to the location pointed by record's data (dataPtr)
			strncpy(dataPtr, value->v.stringV, length);
			dataPtr = dataPtr + schema->typeLength[attrNum];
		  	break;
		}

		case DT_INT:
		{
			// Setting attribute value of an attribute of type INTEGER
			*(int *) dataPtr = value->v.intV;	  
			dataPtr = dataPtr + sizeof(int);
		  	break;
		}
		
		case DT_FLOAT:
		{
			// Setting attribute value of an attribute of type FLOAT
			*(float *) dataPtr = value->v.floatV;
			dataPtr = dataPtr + sizeof(float);
			break;
		}
		
		case DT_BOOL:
		{
			// Setting attribute value of an attribute of type STRING
			*(bool *) dataPtr = value->v.boolV;
			dataPtr = dataPtr + sizeof(bool);
			break;
		}

		default:
			printf("Serializer not defined for the given datatype. \n");
			break;
	}			
	return RC_OK;
}