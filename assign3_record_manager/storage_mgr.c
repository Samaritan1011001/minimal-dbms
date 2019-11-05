#include <stdio.h>
#include "storage_mgr.h"
#include <stdlib.h>
#include <sys/stat.h>

#define FILE_NAME "dbFile.bin"

// Pointer to file
FILE *filePointer;

// File header page
SM_FileHandle fileHeader;

// Memory Allocation Method
char *getPointerWithAllocatedMemory()
{
	char *ptr;
	// Dynamic memory allocation using calloc, and the size of memory allocated is equal to PAGE_SIZE
	ptr = (char *)calloc(PAGE_SIZE, sizeof(char));

	if (ptr == NULL)
	{   
		// If pointer is Null it means memory allocation failed
		printf("Memory not allocated.\n");
		exit(0);
	}
	else
	{
		// Memory has been successfully allocated
		//printf("Memory successfully allocated using calloc.\n");
		return ptr;
	}
}

//Initialization method
void initStorageManager()
{
	printf("Everything set up in Storage manager\n");
}
// Creating a new Page file method
RC createPageFile(char *fileName)
{
	SM_PageHandle emptyPage;
	//creating an empty page
	emptyPage = (char *)getPointerWithAllocatedMemory();
	// Opening the file in "w+" mode : creates (if file does not exists) or open a file for reading and writing
	filePointer = fopen(fileName, "w+");
	if (filePointer == NULL)
	{	
		//If filepointer is Null then there is an error opening the file
		fprintf(stderr, "\nError open file\n");
		return RC_FILE_HANDLE_NOT_INIT;
	}
	//Initializing the fileHeader parameters
	fileHeader.curPagePos = 0;
	fileHeader.fileName = fileName;
	fileHeader.totalNumPages = 1;

	//Writing the page which was created above
	fwrite(&fileHeader, sizeof(struct SM_FileHandle), 1, filePointer);
	fwrite(emptyPage, sizeof(char), PAGE_SIZE, filePointer);

	// printf("Contents to file written successfully !\n");

	// closing the file
	fclose(filePointer);
	filePointer = NULL;
	free(emptyPage);
	return RC_OK;
}

//Opening the page file method
RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{

	// printf("File name -> %s", fileName);
	//Opening the page file in "r" mode : opens the file in the read mode
	filePointer = fopen(fileName, "r");
	//If filepointer is Null then there is an error opening the file
	if (filePointer == NULL)
	{
		fprintf(stderr, "\nError open file\n");
		return RC_FILE_NOT_FOUND;
	}

	else
	{
		fread(&fileHeader, sizeof(struct SM_FileHandle), 1, filePointer);
		//Updating the fHandle parameters
		fHandle->curPagePos = fileHeader.curPagePos;
		fHandle->fileName = fileHeader.fileName;
		fHandle->mgmtInfo = fileHeader.mgmtInfo;
		fHandle->totalNumPages = fileHeader.totalNumPages;
		// printf("total no of pages: %d\n", fHandle->totalNumPages);

		return RC_OK;
	}
}

//Closing the page file method
RC closePageFile(SM_FileHandle *fHandle)
{
	// Closing the page using the inbuilt fclose function
	fclose(filePointer);
	filePointer = NULL;
	// printf("Page file Closed successfully \n");
	return RC_OK;
}

//Destroying page file method
RC destroyPageFile(char *fileName)
{	
	//Destroying the page file using the inbuilt remove function, the file is can't be accessed anymore
	remove(fileName);
	// printf("File deleted successfully\n");
	return RC_OK;
}

//Setting up the header page in the file, which holds all the informaton ( totalNumPages, curPagePos,mgmtInfo,fileName)
RC getFileHeader(SM_FileHandle *fHandle)
{	//Opening the file in "r+" mode : Opens the file in read and write mode
	// filePointer = fopen(fHandle->fileName, "r+");
	// Moving the file pointer to the start of page. SEEK_SET denotes starting of the file.
	fseek(filePointer, 0, SEEK_SET);
	fread(&fileHeader, sizeof(struct SM_FileHandle), 1, filePointer);
	fHandle->totalNumPages = fileHeader.totalNumPages;
	fHandle->curPagePos = fileHeader.curPagePos;
	fHandle->mgmtInfo = fileHeader.mgmtInfo;
	fHandle->fileName = fileHeader.fileName;
	return RC_OK;
}

//Updating the file header 
RC updateFileHeader(SM_FileHandle *fHandle)
{	//Opening the file in "r+" mode : Opens the file in read and write mode
	// filePointer = fopen(fHandle->fileName, "r+");
	// Moving the file pointer to the start of page. SEEK_SET denotes starting of the file
	int successful = fseek(filePointer, 0, SEEK_SET);
	//Cheks if fseek was successful
	if (successful == 0)
	{	
		// printf("Current page pos -> %i\n",fHandle->curPagePos);
		//Updating the parameters
		fwrite(fHandle, sizeof(struct SM_FileHandle), 1, filePointer);
		return RC_OK;
	}
	else
	{
		return RC_WRITE_FAILED;
	}
}

/* reading blocks from disc */
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	int openFileResult = 0;
	//Opening the page file
	if (filePointer == NULL)
	{
		openFileResult = openPageFile(fHandle->fileName, fHandle);
	}
	//Check if file was opended successfully
	if (openFileResult == 0)
	{
		//If yes then we get the all the header information
		getFileHeader(fHandle);

		//To check if the pageNum is less than the totalNumPages present and is greater than  0, then we seek the content and store it in the location pointed by memPage
		if (pageNum <= fHandle->totalNumPages || pageNum >= 0)
		{
			//Moving the pointer position, we are addding (pageNum * PAGE_SIZE) in the offeset field because for each page created we need to sfit the pointer position.
			int successful = fseek(filePointer, sizeof(struct SM_FileHandle) + (pageNum * PAGE_SIZE), SEEK_SET);
			// If the seek was successful, we read the file content
			if (successful == 0)
			{
				//Reading the content
				fread(memPage, sizeof(char), PAGE_SIZE, filePointer);
				fHandle->curPagePos = pageNum;
				// printf("mempage -> %c", memPage[0]);
				//Updating the header information
				RC updateSuccess = updateFileHeader(fHandle);
				if (RC_OK == updateSuccess)
				{	//Closing the file if updating was successful
					closePageFile(fHandle);
					return RC_OK;
				}
				else
				{
					return updateSuccess;
				}
			}
			else
			{	//If seek is not successful, we give an error
				return RC_READ_NON_EXISTING_PAGE;
			}

		}
		else
		{	//If the pageNum specified is not valid, we give an error
			return RC_READ_NON_EXISTING_PAGE;
		}
	}
	else
	{	//If file was not opended successfully we give an error
		return RC_FILE_NOT_FOUND;
	}
}

//Getting the current postion of the page
int getBlockPos(SM_FileHandle *fHandle)
{
	if (filePointer == NULL)
	{	
		//Opening the file in read mode
		filePointer = fopen(fHandle->fileName, "r");
	}
	//Reading the file header and returning the curPagePos 
	fread(&fileHeader, sizeof(struct SM_FileHandle), 1, filePointer);

	return fileHeader.curPagePos;
}

RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	// Using the readBlock function to read the first block (starting from the 0th position)
	return readBlock(0, fHandle, memPage);
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	// Using the readBlock function to read the previous block (Subtracting one from current page position gives us previous page position)
	int pagePos = getBlockPos(fHandle);
	// printf("pagenum prev-> %i\n", pagePos);
	return readBlock(pagePos - 1, fHandle, memPage);
}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	// Using the readBlock function to read the Current block (current page position is passed in this case)
	int pagePos = getBlockPos(fHandle);
	return readBlock(pagePos, fHandle, memPage);
}

RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	// Using the readBlock function to read the next block (Adding one to current page position gives us next page position)
	int pagePos = getBlockPos(fHandle);
	// printf("pagenum next-> %i\n", pagePos);
	return readBlock(pagePos + 1, fHandle, memPage);
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	// Using the readBlock function to read the last block (totalNumPages - 1 gives us the postion of the last block)
	return readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
}

RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{

	filePointer = fopen(fHandle->fileName, "r+");
	// getFileHeader(fHandle);
	// printf("total no of pages in write block 0 : %d\n", fHandle->totalNumPages);

	// printf("pageNum -> %i\n",pageNum);
	// printf("fHandle->totalNumPages -> %i\n",fHandle->totalNumPages);
	if (pageNum <= fHandle->totalNumPages && pageNum >= 0)
	{
		int successful = fseek(filePointer, sizeof(struct SM_FileHandle) + (pageNum * PAGE_SIZE), SEEK_SET);
		// int successful = fseek(filePointer, 0, SEEK_END);

		if (successful == 0)
		{
			// printf("File pointer -> %li\n", ftell(filePointer));
			// printf("mem page in write block -> %c\n", memPage[0]);
			fwrite(memPage, sizeof(char), PAGE_SIZE, filePointer);
			fHandle->curPagePos = pageNum;
			// printf("File size -> %li\n", ftell(filePointer));

			// Calculate the total number of pages everytime so there is no inconsistency 
			fHandle->totalNumPages = (ftell(filePointer) - sizeof(struct SM_FileHandle)) / PAGE_SIZE;

			// printf("total no of pages in write block 1 : %d\n", fHandle->totalNumPages);

			RC updateSuccess = updateFileHeader(fHandle);
			if (RC_OK == updateSuccess)
			{
				closePageFile(fHandle);
				return RC_OK;
			}
			else
			{
				return updateSuccess;
			}
		}
		else
		{
			return RC_WRITE_FAILED;
		}
	}else{
		return RC_WRITE_FAILED;
	}
	// // Checking if the pageNumber parameter is less than Total number of pages and less than 0, then return respective error code
	// if (pageNum > fHandle->totalNumPages || pageNum < 0)
    //     	return RC_WRITE_FAILED;
	
	// // Opening file stream in read & write mode. 'r+' mode opens the file for both reading and writing.	
	// filePointer = fopen(fHandle->fileName, "r+");
	
	// // Checking if file was successfully opened.
	// if(filePointer == NULL)
	// 	return RC_FILE_NOT_FOUND;

	// int startPosition = pageNum * PAGE_SIZE;

	// if(pageNum == 0) { 
	// 	//Writing data to non-first page
	// 	fseek(filePointer, startPosition, SEEK_SET);	
	// 	int i;
	// 	for(i = 0; i < PAGE_SIZE; i++) 
	// 	{
	// 		// Checking if it is end of file. If yes then append an enpty block.
	// 		if(feof(filePointer)) // check file is ending in between writing
	// 			 appendEmptyBlock(fHandle);
	// 		// Writing a character from memPage to page file			
	// 		fputc(memPage[i], filePointer);
	// 	}

	// 	// Setting the current page position to the cursor(pointer) position of the file stream
	// 	fHandle->curPagePos = ftell(filePointer); 

	// 	// Closing file stream so that all the buffers are flushed.
	// 	fclose(filePointer);	
	// } else {	
	// 	// Writing data to the first page.
	// 	fHandle->curPagePos = startPosition;
	// 	fclose(filePointer);
	// 	writeCurrentBlock(fHandle, memPage);
	// }
	// return RC_OK;
}

RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{ // Getting the current position position
	int pg_pos = getBlockPos(fHandle);
	//Performing the write operation , and it will return RC_OK if writeBlockis successful
	return writeBlock(pg_pos, fHandle, memPage);
}

RC appendEmptyBlock(SM_FileHandle *fHandle)
{
	SM_PageHandle append_EmptyBlock;
	//creating an empty page
	append_EmptyBlock = (char *)getPointerWithAllocatedMemory();
	int successful = fseek(filePointer, sizeof(struct SM_FileHandle) + (fHandle->totalNumPages * PAGE_SIZE), SEEK_SET);
	if (successful == 0)
	{
		//Appending the page created to the file by writing that page
		fwrite(append_EmptyBlock, sizeof(char), PAGE_SIZE, filePointer);
		int totalPages = (ftell(filePointer) - sizeof(struct SM_FileHandle)) / PAGE_SIZE;
		fHandle->curPagePos = totalPages;
		fHandle->totalNumPages = totalPages;
			// printf("Total pages in append function : %i", fHandle->totalNumPages);

		RC updateSuccess = updateFileHeader(fHandle);
		if (RC_OK == updateSuccess)
		{
			// closePageFile(fHandle);
			return RC_OK;
		}
		else
		{
			return updateSuccess;
		}
	}
	else
	{
		free(append_EmptyBlock);
		return RC_WRITE_FAILED;
	}
}

RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle)
{

	getFileHeader(fHandle);
	// We need to append new pages only if numberOfPages exceeds totalNumPages, hence checking for it.
	// printf("Total pages before : %i", fHandle->totalNumPages);
	if (numberOfPages > fHandle->totalNumPages)
	{ // As the numberOfPages exceeds totalNumPages we append new pages (blocks)
		while (numberOfPages > fHandle->totalNumPages)
			//Appending the new pages(blocks)
			appendEmptyBlock(fHandle);

		// printf("Total pages after : %i", fHandle->totalNumPages);

		return RC_OK;
	}
	else
	{ //numberOfPages does not exceeds totalNumPages hence no need to append pages, Capacity ensured.
		return RC_OK;
	}
}
