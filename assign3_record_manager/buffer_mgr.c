// #include <stdio.h>
// #include "storage_mgr.h"
// #include "buffer_mgr.h"
// #include <stdlib.h>
// #include <sys/stat.h>

// //This tells us the size of the buffer pool, the number of page frames that can be contained in the buffer pool.
// int buffer_pool_size = 0;

// int pages_read = 0;
// int page_hits =0;
// int WC=0;

// // Keep track if buffer is init or shutdown
// bool buffer_active = false;

// //The actual structure of the page frame inside the buffer pool
// typedef struct pg_frame
// {
//     SM_PageHandle data;
//     PageNumber pageNum;
//     int Dirty_bit;
//     int fix_count;
//     int no_of_hits;
// } pg_frame;


// //FIFO
// void FIFO(BM_BufferPool *const bm, pg_frame *page)
// {
// 	pg_frame *pageFrame = (pg_frame *) bm->mgmtData;
	
// 	int i = 0;
// 	int front_id;
// 	front_id = pages_read % buffer_pool_size;

// 	while(i < buffer_pool_size)
// 	{	//Check if Page frame being used by some client
// 		if(pageFrame[front_id].fix_count == 0)
// 		{
// 			//  We need to check if that page is modified.
//     		// If yes then we first need to write that page back and then replace the page.
// 			if(pageFrame[front_id].Dirty_bit == 1)
// 			{
// 				SM_FileHandle fh;
// 				//Opening the page file 
// 				openPageFile(bm->pageFile, &fh);
// 				//Writing the data to disk
// 				writeBlock(pageFrame[front_id].pageNum, &fh, pageFrame[front_id].data);
				
// 				// To keep a count of number of write operations (Statistics functions)
// 				WC = WC + 1;
// 			}
			
// 			// Replacing the values to new one
// 			pageFrame[front_id].data = page->data;
// 			pageFrame[front_id].pageNum = page->pageNum;
// 			pageFrame[front_id].Dirty_bit = page->Dirty_bit;
// 			pageFrame[front_id].fix_count = page->fix_count;
// 			break;
// 		}
// 		else
// 		{
// 			// Page frame being used by some client as fix cpunt is not 0, hence we go to next page frame
// 			front_id = front_id+1;
// 			if(front_id % buffer_pool_size == 0)
// 				front_id = 0;
// 			else
// 				front_id = front_id;
// 		}
// 		i++;
// 	}
// }


// //LRU strategy page replacement strategy
// RC LRU(BM_BufferPool *const bm,pg_frame *Page)
// {
//     int i=0;
// 	pg_frame *frame = (pg_frame*)bm->mgmtData;
//     int least_no_of_hits = 0;
//     int LRU_ind =0 ;

//     while(i < buffer_pool_size)
// 	{   
//         //If fix_count is not 0, that indicates the page frame is being used by some client and we cant replace it
// 		if(frame[i].fix_count != 0)
// 		{
// 			continue;
// 		}
//         //Else fix_count is 0 and we can replace the page
//         else
//         {   //initialize the index and least_no_of_hits for least recently used page
//             LRU_ind = i;
// 		    least_no_of_hits = frame[i].no_of_hits;
// 			break;
//         }
//         i=i+1;
// 	}	
//     //From all the page frames that can be replaced we find the page frame which can be replaced, that is the one with least no of hits
//     for(i = LRU_ind + 1; i < buffer_pool_size; i++)
// 	{   //Update the LRU_ind and least_no_of_hits
// 		if(frame[i].no_of_hits < least_no_of_hits)
// 		{
// 			LRU_ind = i;
// 			least_no_of_hits = frame[i].no_of_hits;
// 		}
// 	}

//     //Now we have the page that can be replaced, but we need to check if that page is modified.
//     // If yes then we first need to write that page back and then replace the page.

// 	//Check if page is dirty (modified)
//     if(frame[LRU_ind].Dirty_bit == 1)
// 	{
// 		SM_FileHandle *fHandle;
// 		//Opening the page file 
//         openPageFile(bm->pageFile, fHandle);
//         //Writing the data to disk
//         writeBlock(frame[LRU_ind].pageNum, fHandle, frame[LRU_ind].data);
//         //As the page file has been written successfully, it is no longer dirty.
//         frame[i].Dirty_bit = 0;
// 		WC = WC + 1;
// 	}

//     //Replace the page
//     frame[LRU_ind].data = Page->data;
// 	frame[LRU_ind].pageNum = Page->pageNum;
// 	frame[LRU_ind].Dirty_bit = Page->Dirty_bit;
// 	frame[LRU_ind].fix_count = Page->fix_count;
// 	frame[LRU_ind].no_of_hits = Page->no_of_hits;

// }

// // Buffer Manager Interface Pool Handling
// RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
//                   const int numPages, ReplacementStrategy strategy,
//                   void *stratData)
// {
// 	struct stat buffer;   
//   	if (!fopen(pageFileName, "r")){
// 		//   printf("File not exist\n");
// 		  return RC_FILE_NOT_FOUND;
// 	  }

// 	buffer_active = true;
//     //Initializing the values to the respective values passed
//     bm->pageFile = (char *)pageFileName;
//     bm->numPages = numPages;
//     bm->strategy = strategy;
//     //number of pages contained inside the buffer pool
//     buffer_pool_size = numPages;
//     //Setting the size of Pages (which is of type pg_frame).
//     //It will be equal to size of structure (pg_frame) * numPages
//     pg_frame *Pages = (pg_frame *)calloc(numPages, sizeof(pg_frame));
//     //Storing a pointer to the area in memory that stores the page frames
//     bm->mgmtData = Pages;
// 	WC = 0;

//     // printf("here -> %d\n", buffer_pool_size);
//     //Initialization for each page in the buffer pool
//     for (int i = 0; i < buffer_pool_size; i++)
//     { 
// 		//Initially all pages should be empty hence data = NULL
//         Pages[i].data = NULL;
//         //Initially no. of pages
//         Pages[i].pageNum = -1;
//         //No modifications to any pages hence dirty bit is 0
//         Pages[i].Dirty_bit = 0;
//         //No client has pinned any pages yet hence fix count is 0
//         Pages[i].fix_count = 0;
// 		//Initialize no_of_hits to 0
//         Pages[i].no_of_hits = 0;
//         // printf("pages in init -> %d\n", Pages[i].fix_count);
//     }
//     return RC_OK;
// }

// //To free up all resources associated with buffer pool
// RC shutdownBufferPool(BM_BufferPool *const bm)
// {

// 	if(!buffer_active)
// 		return RC_UNABLE_TO_REMOVE;
//     int i = 0;
//     pg_frame *frames = (pg_frame *)bm->mgmtData;
//     // printf("here -> %d\n", buffer_pool_size);

//     forceFlushPool(bm);
//     //Check if there are any pinned pages in the buffer
//     while (i < buffer_pool_size)
//     {
//         // printf("pages in shut for %d-> %d\n",i,frames[i].fix_count);
//         //if the fix count value = 0 then there is no client using that page (hence these pages can be removed from the buffer pool)
//         if (frames[i].fix_count == 0)
//         {
//             // printf("No pinned pages in the frame %d so we can free the resources\n", i);
//         }
//         else
//         {
//             // printf("There are pinned pages in the frame %d, hence we cannot free the resources\n", i);
//             return RC_UNABLE_TO_REMOVE;
//         }
//         i++;
//     }
//     // If there are no pinned pages in any of the frames we can free them up
//     free(frames);
//     //As we freed our frames the pointer should be now initialized to NULL as the frame does not exist
//     bm->mgmtData = NULL;
// 	buffer_active = false;
//     return RC_OK;
// }
// //all dirty pages (with fix count 0) from the buffer pool to be written to disk
// RC forceFlushPool(BM_BufferPool *const bm)
// {
// 	if(!buffer_active)
// 		return RC_UNABLE_TO_REMOVE;
//     SM_FileHandle fHandle;
//     int i = 0;
//     pg_frame *frames = (pg_frame *)bm->mgmtData;
//     while (i < buffer_pool_size)
//     { //if the fix count value = 0 then there is no client using that page
//         if (frames[i].fix_count == 0)
//         {
//             // If the dirty bit = 1 then we need to open the page file and write the contents to the page file on disk
//             if (frames[i].Dirty_bit == 1)
//             {
// 				// Checking if page is in memory
//                 //Opening the page file
//                 RC a = openPageFile(bm->pageFile, &fHandle);
//                 //Writing the data to disk
//                 writeBlock(frames[i].pageNum, &fHandle, frames[i].data);
//                 //As the page file has been return successfully, it is no longer dirty.
//                 frames[i].Dirty_bit = 0;
// 				WC = WC + 1;
// 				closePageFile(&fHandle);
//             }
//         }
//         i++;
//     }
//     return RC_OK;
// }

// RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page,
//            const PageNumber pageNum)
// {
// 	if(!buffer_active)
// 		return RC_UNABLE_TO_REMOVE;
// 	if(pageNum < 0){
// 	return RC_ERROR_UNABLE_FORCING_PAGE;
// 	}

// 	pg_frame *frames = (pg_frame *)bm->mgmtData;
//     pg_frame *pool_frame = (pg_frame *) malloc(sizeof(pg_frame));
	
// 	// If there are no pages in the buffer pool , then we pin the first page to pool
// 	if(frames[0].pageNum == -1)
// 	{
// 		// initializing of page of buffer pool
// 		pages_read = 0;
//         page_hits= 0;
// 		SM_FileHandle fh;
// 		openPageFile(bm->pageFile, &fh);
// 		frames[0].data = (SM_PageHandle) calloc(1,PAGE_SIZE);
// 		ensureCapacity(pageNum,&fh);
// 		readBlock(pageNum, &fh, frames[0].data);
// 		frames[0].pageNum = pageNum;
// 		frames[0].fix_count = frames[0].fix_count +1;
// 		frames[0].no_of_hits = 0;	
// 		page->pageNum = pageNum;
// 		page->data = frames[0].data;
// 		closePageFile(&fh);
		
// 		return RC_OK;		
// 	}
// 	else
// 	{	
// 		bool BF = true;
// 		int i=0;
		
// 		while( i < buffer_pool_size)
// 		{
// 			if(frames[i].pageNum != -1)
// 			{	
// 				if(frames[i].pageNum == pageNum)
// 				{
// 				// Checking if page is in memory
// 					// printf("Page in mem\n");
// 					// Incrementing fix count, as the page is being accessed by an additional client
// 					frames[i].fix_count ++;
// 					BF = false;
// 					page_hits = page_hits + 1; //Keep count of hits

// 					if(bm->strategy == RS_LRU)
// 						// Update no of hits for that page for LRU	
// 						frames[i].no_of_hits = page_hits;
					
// 					page->pageNum = pageNum;
// 					page->data = frames[i].data;
// 					break;
// 				}

// 			} 
// 			else 
// 			{
// 				// printf("Here\n");
// 				SM_FileHandle fh;
// 				//Open Page File
// 				openPageFile(bm->pageFile, &fh);
// 				frames[i].data = (SM_PageHandle) calloc(1,PAGE_SIZE);
// 				//Read Page file
// 				readBlock(pageNum, &fh, frames[i].data);
// 				pages_read = pages_read +1;	// no of read occured for staticstics function
// 				page_hits = page_hits + 1; //Keep count of hits
// 				//Set pageNum and fix_count
// 				frames[i].pageNum = pageNum;
// 				frames[i].fix_count = 1;

// 				if(bm->strategy == RS_LRU)
// 					// Update no of hits for that page for LRU
// 					frames[i].no_of_hits = page_hits;				
// 				page->pageNum = pageNum;
// 				page->data = frames[i].data;
				
// 				BF= false;

// 				break;
// 			}
// 			i=i+1;
// 		}
		
// 		// We must choose a replacement strategy to replace the page if the buffer is full
// 		if(BF == true)
// 		{
// 			// printf("Buffer full for page num %i\n", pageNum);

// 			int pinned_frames = 0;

// 			for(int i =0; i< buffer_pool_size;i++){
// 				if(frames[i].fix_count>0){
// 					pinned_frames++;
// 				}
// 			}
// 			if(pinned_frames==buffer_pool_size){
// 				return RC_ERROR_UNABLE_TO_UNPIN_PAGE;
// 			}
// 			// Updating the buffer pool
// 			SM_FileHandle fh;
// 			//Open Page File
// 			openPageFile(bm->pageFile, &fh);
// 			pool_frame->data = (SM_PageHandle) calloc(1,PAGE_SIZE);
// 			//Read Page file
// 			readBlock(pageNum, &fh, pool_frame->data);
// 			pages_read = pages_read +1;	// no of read occured for staticstics function
// 			page_hits = page_hits + 1; //Keep count of hits
// 			pool_frame->pageNum = pageNum;
// 			pool_frame->Dirty_bit = 0;		
// 			pool_frame->fix_count = 1;
// 			//Close Page file
// 			closePageFile(&fh);
			

// 			if(bm->strategy == RS_LRU)
// 				// Update no of hits for that page for LRU
// 				pool_frame->no_of_hits = page_hits;				
// 			page->pageNum = pageNum;
// 			page->data = pool_frame->data;			

// 			// Choose the replacement strategy to be used
// 			switch(bm->strategy)
// 			{	// FIFO Implementation		
// 				case RS_FIFO: 
// 					FIFO(bm, pool_frame);
// 					break;
// 				// LRU implementation
// 				case RS_LRU: 
// 					LRU(bm, pool_frame);
// 					break;
				
// 				default:
// 					printf("\nStrategy not available ");
// 					break;
// 			}
						
// 		}		
// 		return RC_OK;
// 	}	
// }



// //The buffer manager needs to know whether the page was modified by the client, If yes then we mark that page as dirty
// RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
// {
//     int i=0;
// 	pg_frame *frame = (pg_frame*)bm->mgmtData;
//     while(i < buffer_pool_size)
//     {   //Check whether the current page (pg_frame[i].pageNum) is the one that needs to be marked dirty 
//         if(frame[i].pageNum == page->pageNum)
//         {   // If yes we mark that page as dirty by setting its Dirty_bit to 1
//             frame[i].Dirty_bit = 1;
//             return RC_OK;       
//         }
//         i = i+1;           
//     }  
//     //If we dont find the page in buffer pool then we send an error      
//     return RC_ERROR_UNABLE_TO_MAKE_PAGE_DIRTY;
// }

// //When the client no longer needs the page we need to unpin the page (remove the page from buffer pool)
// RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
// {
//     int i=0;
// 	pg_frame *frame = (pg_frame*)bm->mgmtData;
// 	printf("buffer_pool_size -> %i\n",buffer_pool_size);
//     while(i < buffer_pool_size)
//     {   
// 		printf("frame[i].pageNum -> %i\n",frame[i].pageNum);
// 		printf("page->pageNum -> %i\n",page->pageNum);

// 		//Check whether the current page (pg_frame[i].pageNum) is the one that needs to be unpinned
//         if(frame[i].pageNum == page->pageNum)
//         {       // If yes then we reduce its fix count by 1 (It indicates that the page is not need by this client)
//                 frame[i].fix_count = frame[i].fix_count - 1;
				
// 				//We come out of the loop 
//                 return RC_OK;
//         }
//         i = i+1; 
//     }
// 	printf("Here2\n");
//     //If we dont find the page in buffer pool then we send an error 
//     return RC_ERROR_UNABLE_TO_UNPIN_PAGE;

// }

// //The dirty pages (modified pages) should be written back to the page file on disk
// RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
// {   
//     SM_FileHandle fHandle;
//     int i=0;
// 	pg_frame *frame = (pg_frame*)bm->mgmtData;
//     while(i < buffer_pool_size)
//     {//Check whether the current page (pg_frame[i].pageNum) is the one that needs to be written back
//         if(frame[i].pageNum == page->pageNum)
//         {   //If yes we open the page file
// 			// printf("Here!! \n");
//             openPageFile(bm->pageFile, &fHandle);
//             //Writing the data to disk
//             writeBlock(frame[i].pageNum, &fHandle, frame[i].data);
//             //As the page file has been written successfully, it is no longer dirty.
//             frame[i].Dirty_bit = 0;
// 			WC = WC + 1;
//             return RC_OK;
//         }
//         i = i+1; 
//     }
//     return RC_ERROR_UNABLE_FORCING_PAGE;
// }

// // The getFrameContents function returns an array of PageNumbers (of size numPages) where the ith element is the number of the page stored in the ith page frame
// PageNumber *getFrameContents (BM_BufferPool *const bm)
// {	
// 	int i=0;
// 	pg_frame *frame = (pg_frame*)bm->mgmtData;
// 	PageNumber *FCon = calloc(1,sizeof(PageNumber) * buffer_pool_size);
	
// 	while(i < buffer_pool_size) 
// 	{	
// 		if(frame[i].pageNum == -1){
// 			FCon[i] = NO_PAGE;
// 		}
// 		else {
// 			FCon[i] = frame[i].pageNum;
// 			// printf("FCon[i] -> %d\n",frame[i].pageNum);
// 		}
// 		i++;
// 	}
// 	// printf("Fcon -> %i\n",FCon);
// 	return FCon;
// }

// // The getDirtyFlags function returns an array of bools (of size numPages) where the ith element is TRUE if the page stored in the ith page frame is dirty.
// bool *getDirtyFlags (BM_BufferPool *const bm)
// {	
// 	int i=0;
// 	pg_frame *frame = (pg_frame*)bm->mgmtData;
// 	bool *DF = calloc(1,sizeof(bool) * buffer_pool_size);

// 	while(i < buffer_pool_size) 
// 	{	
// 		if(frame[i].Dirty_bit == 1)
// 			DF[i] = TRUE;
// 		else
// 			DF[i] = FALSE;
// 		i++;
// 	}
// 	return DF;
// }

// // The getFixCounts function returns an array of ints (of size numPages) where the ith element is the fix count of the page stored in the ith page frame. 
// int *getFixCounts (BM_BufferPool *const bm)
// {
// 	int i=0;
// 	pg_frame *frame = (pg_frame*)bm->mgmtData;
// 	int *FC = calloc(1,sizeof(int) * buffer_pool_size);

// 	while(i < buffer_pool_size) 
// 	{	
// 		if(frame[i].fix_count == -1){
// 			FC[i] = 0;
// 		}
// 		else{
// 			FC[i] = frame[i].fix_count;
// 			// printf("FC[i] -> %d\n",frame[i].fix_count);

// 		}
// 		i++;
// 	}
// 	return FC;
// }

// //The getNumReadIO function returns the number of pages that have been read from disk since a buffer pool has been initialized.
// int getNumReadIO (BM_BufferPool *const bm)
// {
// 	return (pages_read + 1);
// }

// // The getNumWriteIO returns the number of pages written to the page file since the buffer pool has been initialized
// int getNumWriteIO (BM_BufferPool *const bm)
// {
// 	return WC;
// }

#include<stdio.h>
#include<stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <math.h>

// This structure represents one page frame in buffer pool (memory).
typedef struct Page
{
	SM_PageHandle data; // Actual data of the page
	PageNumber pageNum; // An identification integer given to each page
	int dirtyBit; // Used to indicate whether the contents of the page has been modified by the client
	int fixCount; // Used to indicate the number of clients using that page at a given instance
	int hitNum;   // Used by LRU algorithm to get the least recently used page	
	int refNum;   // Used by LFU algorithm to get the least frequently used page
} PageFrame;

// "bufferSize" represents the size of the buffer pool i.e. maximum number of page frames that can be kept into the buffer pool
int bufferSize = 0;

// "rearIndex" basically stores the count of number of pages read from the disk.
// "rearIndex" is also used by FIFO function to calculate the frontIndex i.e.
int rearIndex = 0;

// "writeCount" counts the number of I/O write to the disk i.e. number of pages writen to the disk
int writeCount = 0;

// "hit" a general count which is incremented whenever a page frame is added into the buffer pool.
// "hit" is used by LRU to determine least recently added page into the buffer pool.
int hit = 0;

// "clockPointer" is used by CLOCK algorithm to point to the last added page in the buffer pool.
int clockPointer = 0;

// "lfuPointer" is used by LFU algorithm to store the least frequently used page frame's position. It speeds up operation  from 2nd replacement onwards.
int lfuPointer = 0;

// Defining FIFO (First In First Out) function
extern void FIFO(BM_BufferPool *const bm, PageFrame *page)
{
	//printf("FIFO Started");
	PageFrame *pageFrame = (PageFrame *) bm->mgmtData;
	
	int i, frontIndex;
	frontIndex = rearIndex % bufferSize;

	// Interating through all the page frames in the buffer pool
	for(i = 0; i < bufferSize; i++)
	{
		if(pageFrame[frontIndex].fixCount == 0)
		{
			// If page in memory has been modified (dirtyBit = 1), then write page to disk
			if(pageFrame[frontIndex].dirtyBit == 1)
			{
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				writeBlock(pageFrame[frontIndex].pageNum, &fh, pageFrame[frontIndex].data);
				
				// Increase the writeCount which records the number of writes done by the buffer manager.
				writeCount++;
			}
			
			// Setting page frame's content to new page's content
			pageFrame[frontIndex].data = page->data;
			pageFrame[frontIndex].pageNum = page->pageNum;
			pageFrame[frontIndex].dirtyBit = page->dirtyBit;
			pageFrame[frontIndex].fixCount = page->fixCount;
			break;
		}
		else
		{
			// If the current page frame is being used by some client, we move on to the next location
			frontIndex++;
			frontIndex = (frontIndex % bufferSize == 0) ? 0 : frontIndex;
		}
	}
}

// Defining LFU (Least Frequently Used) function
extern void LFU(BM_BufferPool *const bm, PageFrame *page)
{
	//printf("LFU Started");
	PageFrame *pageFrame = (PageFrame *) bm->mgmtData;
	
	int i, j, leastFreqIndex, leastFreqRef;
	leastFreqIndex = lfuPointer;	
	
	// Interating through all the page frames in the buffer pool
	for(i = 0; i < bufferSize; i++)
	{
		if(pageFrame[leastFreqIndex].fixCount == 0)
		{
			leastFreqIndex = (leastFreqIndex + i) % bufferSize;
			leastFreqRef = pageFrame[leastFreqIndex].refNum;
			break;
		}
	}

	i = (leastFreqIndex + 1) % bufferSize;

	// Finding the page frame having minimum refNum (i.e. it is used the least frequent) page frame
	for(j = 0; j < bufferSize; j++)
	{
		if(pageFrame[i].refNum < leastFreqRef)
		{
			leastFreqIndex = i;
			leastFreqRef = pageFrame[i].refNum;
		}
		i = (i + 1) % bufferSize;
	}
		
	// If page in memory has been modified (dirtyBit = 1), then write page to disk	
	if(pageFrame[leastFreqIndex].dirtyBit == 1)
	{
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		writeBlock(pageFrame[leastFreqIndex].pageNum, &fh, pageFrame[leastFreqIndex].data);
		
		// Increase the writeCount which records the number of writes done by the buffer manager.
		writeCount++;
	}
	
	// Setting page frame's content to new page's content		
	pageFrame[leastFreqIndex].data = page->data;
	pageFrame[leastFreqIndex].pageNum = page->pageNum;
	pageFrame[leastFreqIndex].dirtyBit = page->dirtyBit;
	pageFrame[leastFreqIndex].fixCount = page->fixCount;
	lfuPointer = leastFreqIndex + 1;
}

// Defining LRU (Least Recently Used) function
extern void LRU(BM_BufferPool *const bm, PageFrame *page)
{	
	PageFrame *pageFrame = (PageFrame *) bm->mgmtData;
	int i, leastHitIndex, leastHitNum;

	// Interating through all the page frames in the buffer pool.
	for(i = 0; i < bufferSize; i++)
	{
		// Finding page frame whose fixCount = 0 i.e. no client is using that page frame.
		if(pageFrame[i].fixCount == 0)
		{
			leastHitIndex = i;
			leastHitNum = pageFrame[i].hitNum;
			break;
		}
	}	

	// Finding the page frame having minimum hitNum (i.e. it is the least recently used) page frame
	for(i = leastHitIndex + 1; i < bufferSize; i++)
	{
		if(pageFrame[i].hitNum < leastHitNum)
		{
			leastHitIndex = i;
			leastHitNum = pageFrame[i].hitNum;
		}
	}

	// If page in memory has been modified (dirtyBit = 1), then write page to disk
	if(pageFrame[leastHitIndex].dirtyBit == 1)
	{
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		writeBlock(pageFrame[leastHitIndex].pageNum, &fh, pageFrame[leastHitIndex].data);
		
		// Increase the writeCount which records the number of writes done by the buffer manager.
		writeCount++;
	}
	
	// Setting page frame's content to new page's content
	pageFrame[leastHitIndex].data = page->data;
	pageFrame[leastHitIndex].pageNum = page->pageNum;
	pageFrame[leastHitIndex].dirtyBit = page->dirtyBit;
	pageFrame[leastHitIndex].fixCount = page->fixCount;
	pageFrame[leastHitIndex].hitNum = page->hitNum;
}

// Defining CLOCK function
extern void CLOCK(BM_BufferPool *const bm, PageFrame *page)
{	
	//printf("CLOCK Started");
	PageFrame *pageFrame = (PageFrame *) bm->mgmtData;
	while(1)
	{
		clockPointer = (clockPointer % bufferSize == 0) ? 0 : clockPointer;

		if(pageFrame[clockPointer].hitNum == 0)
		{
			// If page in memory has been modified (dirtyBit = 1), then write page to disk
			if(pageFrame[clockPointer].dirtyBit == 1)
			{
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				writeBlock(pageFrame[clockPointer].pageNum, &fh, pageFrame[clockPointer].data);
				
				// Increase the writeCount which records the number of writes done by the buffer manager.
				writeCount++;
			}
			
			// Setting page frame's content to new page's content
			pageFrame[clockPointer].data = page->data;
			pageFrame[clockPointer].pageNum = page->pageNum;
			pageFrame[clockPointer].dirtyBit = page->dirtyBit;
			pageFrame[clockPointer].fixCount = page->fixCount;
			pageFrame[clockPointer].hitNum = page->hitNum;
			clockPointer++;
			break;	
		}
		else
		{
			// Incrementing clockPointer so that we can check the next page frame location.
			// We set hitNum = 0 so that this loop doesn't go into an infinite loop.
			pageFrame[clockPointer++].hitNum = 0;		
		}
	}
}

// ***** BUFFER POOL FUNCTIONS ***** //

/* 
   This function creates and initializes a buffer pool with numPages page frames.
   pageFileName stores the name of the page file whose pages are being cached in memory.
   strategy represents the page replacement strategy (FIFO, LRU, LFU, CLOCK) that will be used by this buffer pool
   stratData is used to pass parameters if any to the page replacement strategy
*/
extern RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		  const int numPages, ReplacementStrategy strategy, 
		  void *stratData)
{
	bm->pageFile = (char *)pageFileName;
	bm->numPages = numPages;
	bm->strategy = strategy;

	// Reserver memory space = number of pages x space required for one page
	PageFrame *page = malloc(sizeof(PageFrame) * numPages);
	
	// Buffersize is the total number of pages in memory or the buffer pool.
	bufferSize = numPages;	
	int i;

	// Intilalizing all pages in buffer pool. The values of fields (variables) in the page is either NULL or 0
	for(i = 0; i < bufferSize; i++)
	{
		page[i].data = NULL;
		page[i].pageNum = -1;
		page[i].dirtyBit = 0;
		page[i].fixCount = 0;
		page[i].hitNum = 0;	
		page[i].refNum = 0;
	}

	bm->mgmtData = page;
	writeCount = clockPointer = lfuPointer = 0;
	return RC_OK;
		
}

// Shutdown i.e. close the buffer pool, thereby removing all the pages from the memory and freeing up all resources and releasing some memory space.
extern RC shutdownBufferPool(BM_BufferPool *const bm)
{
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
	// Write all dirty pages (modified pages) back to disk
	forceFlushPool(bm);

	int i;	
	for(i = 0; i < bufferSize; i++)
	{
		// If fixCount != 0, it means that the contents of the page was modified by some client and has not been written back to disk.
		if(pageFrame[i].fixCount != 0)
		{
			return RC_PINNED_PAGES_IN_BUFFER;
		}
	}

	// Releasing space occupied by the page
	free(pageFrame);
	bm->mgmtData = NULL;
	return RC_OK;
}

// This function writes all the dirty pages (having fixCount = 0) to disk
extern RC forceFlushPool(BM_BufferPool *const bm)
{
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
	
	int i;
	// Store all dirty pages (modified pages) in memory to page file on disk	
	for(i = 0; i < bufferSize; i++)
	{
		if(pageFrame[i].fixCount == 0 && pageFrame[i].dirtyBit == 1)
		{
			SM_FileHandle fh;
			// Opening page file available on disk
			openPageFile(bm->pageFile, &fh);
			// Writing block of data to the page file on disk
			writeBlock(pageFrame[i].pageNum, &fh, pageFrame[i].data);
			// Mark the page not dirty.
			pageFrame[i].dirtyBit = 0;
			// Increase the writeCount which records the number of writes done by the buffer manager.
			writeCount++;
		}
	}	
	return RC_OK;
}


// ***** PAGE MANAGEMENT FUNCTIONS ***** //

// This function marks the page as dirty indicating that the data of the page has been modified by the client
extern RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
	
	int i;
	// Iterating through all the pages in the buffer pool
	for(i = 0; i < bufferSize; i++)
	{
		// If the current page is the page to be marked dirty, then set dirtyBit = 1 (page has been modified) for that page
		if(pageFrame[i].pageNum == page->pageNum)
		{
			pageFrame[i].dirtyBit = 1;
			return RC_OK;		
		}			
	}		
	return RC_ERROR;
}

// This function unpins a page from the memory i.e. removes a page from the memory
extern RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{	
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
	
	int i;
	// Iterating through all the pages in the buffer pool
	for(i = 0; i < bufferSize; i++)
	{
		// If the current page is the page to be unpinned, then decrease fixCount (which means client has completed work on that page) and exit loop
		if(pageFrame[i].pageNum == page->pageNum)
		{
			pageFrame[i].fixCount--;
			break;		
		}		
	}
	return RC_OK;
}

// This function writes the contents of the modified pages back to the page file on disk
extern RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
	
	int i;
	// Iterating through all the pages in the buffer pool
	for(i = 0; i < bufferSize; i++)
	{
		// If the current page = page to be written to disk, then right the page to the disk using the storage manager functions
		if(pageFrame[i].pageNum == page->pageNum)
		{		
			SM_FileHandle fh;
			openPageFile(bm->pageFile, &fh);
			writeBlock(pageFrame[i].pageNum, &fh, pageFrame[i].data);
		
			// Mark page as undirty because the modified page has been written to disk
			pageFrame[i].dirtyBit = 0;
			
			// Increase the writeCount which records the number of writes done by the buffer manager.
			writeCount++;
		}
	}	
	return RC_OK;
}

// This function pins a page with page number pageNum i.e. adds the page with page number pageNum to the buffer pool.
// If the buffer pool is full, then it uses appropriate page replacement strategy to replace a page in memory with the new page being pinned. 
extern RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
	    const PageNumber pageNum)
{
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
	
	// Checking if buffer pool is empty and this is the first page to be pinned
	if(pageFrame[0].pageNum == -1)
	{
		// Reading page from disk and initializing page frame's content in the buffer pool
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		pageFrame[0].data = (SM_PageHandle) malloc(PAGE_SIZE);
		ensureCapacity(pageNum,&fh);
		readBlock(pageNum, &fh, pageFrame[0].data);
		pageFrame[0].pageNum = pageNum;
		pageFrame[0].fixCount++;
		rearIndex = hit = 0;
		pageFrame[0].hitNum = hit;	
		pageFrame[0].refNum = 0;
		page->pageNum = pageNum;
		page->data = pageFrame[0].data;
		
		return RC_OK;		
	}
	else
	{	
		int i;
		bool isBufferFull = true;
		
		for(i = 0; i < bufferSize; i++)
		{
			if(pageFrame[i].pageNum != -1)
			{	
				// Checking if page is in memory
				if(pageFrame[i].pageNum == pageNum)
				{
					// Increasing fixCount i.e. now there is one more client accessing this page
					pageFrame[i].fixCount++;
					isBufferFull = false;
					hit++; // Incrementing hit (hit is used by LRU algorithm to determine the least recently used page)

					if(bm->strategy == RS_LRU)
						// LRU algorithm uses the value of hit to determine the least recently used page	
						pageFrame[i].hitNum = hit;
					else if(bm->strategy == RS_CLOCK)
						// hitNum = 1 to indicate that this was the last page frame examined (added to the buffer pool)
						pageFrame[i].hitNum = 1;
					else if(bm->strategy == RS_LFU)
						// Incrementing refNum to add one more to the count of number of times the page is used (referenced)
						pageFrame[i].refNum++;
					
					page->pageNum = pageNum;
					page->data = pageFrame[i].data;

					clockPointer++;
					break;
				}				
			} else {
				SM_FileHandle fh;
				openPageFile(bm->pageFile, &fh);
				pageFrame[i].data = (SM_PageHandle) malloc(PAGE_SIZE);
				readBlock(pageNum, &fh, pageFrame[i].data);
				pageFrame[i].pageNum = pageNum;
				pageFrame[i].fixCount = 1;
				pageFrame[i].refNum = 0;
				rearIndex++;	
				hit++; // Incrementing hit (hit is used by LRU algorithm to determine the least recently used page)

				if(bm->strategy == RS_LRU)
					// LRU algorithm uses the value of hit to determine the least recently used page
					pageFrame[i].hitNum = hit;				
				else if(bm->strategy == RS_CLOCK)
					// hitNum = 1 to indicate that this was the last page frame examined (added to the buffer pool)
					pageFrame[i].hitNum = 1;
						
				page->pageNum = pageNum;
				page->data = pageFrame[i].data;
				
				isBufferFull = false;
				break;
			}
		}
		
		// If isBufferFull = true, then it means that the buffer is full and we must replace an existing page using page replacement strategy
		if(isBufferFull == true)
		{
			// Create a new page to store data read from the file.
			PageFrame *newPage = (PageFrame *) malloc(sizeof(PageFrame));		
			
			// Reading page from disk and initializing page frame's content in the buffer pool
			SM_FileHandle fh;
			openPageFile(bm->pageFile, &fh);
			newPage->data = (SM_PageHandle) malloc(PAGE_SIZE);
			readBlock(pageNum, &fh, newPage->data);
			newPage->pageNum = pageNum;
			newPage->dirtyBit = 0;		
			newPage->fixCount = 1;
			newPage->refNum = 0;
			rearIndex++;
			hit++;

			if(bm->strategy == RS_LRU)
				// LRU algorithm uses the value of hit to determine the least recently used page
				newPage->hitNum = hit;				
			else if(bm->strategy == RS_CLOCK)
				// hitNum = 1 to indicate that this was the last page frame examined (added to the buffer pool)
				newPage->hitNum = 1;

			page->pageNum = pageNum;
			page->data = newPage->data;			

			// Call appropriate algorithm's function depending on the page replacement strategy selected (passed through parameters)
			switch(bm->strategy)
			{			
				case RS_FIFO: // Using FIFO algorithm
					FIFO(bm, newPage);
					break;
				
				case RS_LRU: // Using LRU algorithm
					LRU(bm, newPage);
					break;
				
				case RS_CLOCK: // Using CLOCK algorithm
					CLOCK(bm, newPage);
					break;
  				
				case RS_LFU: // Using LFU algorithm
					LFU(bm, newPage);
					break;
  				
				case RS_LRU_K:
					printf("\n LRU-k algorithm not implemented");
					break;
				
				default:
					printf("\nAlgorithm Not Implemented\n");
					break;
			}
						
		}		
		return RC_OK;
	}	
}


// ***** STATISTICS FUNCTIONS ***** //

// This function returns an array of page numbers.
extern PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	PageNumber *frameContents = malloc(sizeof(PageNumber) * bufferSize);
	PageFrame *pageFrame = (PageFrame *) bm->mgmtData;
	
	int i = 0;
	// Iterating through all the pages in the buffer pool and setting frameContents' value to pageNum of the page
	while(i < bufferSize) {
		frameContents[i] = (pageFrame[i].pageNum != -1) ? pageFrame[i].pageNum : NO_PAGE;
		i++;
	}
	return frameContents;
}

// This function returns an array of bools, each element represents the dirtyBit of the respective page.
extern bool *getDirtyFlags (BM_BufferPool *const bm)
{
	bool *dirtyFlags = malloc(sizeof(bool) * bufferSize);
	PageFrame *pageFrame = (PageFrame *)bm->mgmtData;
	
	int i;
	// Iterating through all the pages in the buffer pool and setting dirtyFlags' value to TRUE if page is dirty else FALSE
	for(i = 0; i < bufferSize; i++)
	{
		dirtyFlags[i] = (pageFrame[i].dirtyBit == 1) ? true : false ;
	}	
	return dirtyFlags;
}

// This function returns an array of ints (of size numPages) where the ith element is the fix count of the page stored in the ith page frame.
extern int *getFixCounts (BM_BufferPool *const bm)
{
	int *fixCounts = malloc(sizeof(int) * bufferSize);
	PageFrame *pageFrame= (PageFrame *)bm->mgmtData;
	
	int i = 0;
	// Iterating through all the pages in the buffer pool and setting fixCounts' value to page's fixCount
	while(i < bufferSize)
	{
		fixCounts[i] = (pageFrame[i].fixCount != -1) ? pageFrame[i].fixCount : 0;
		i++;
	}	
	return fixCounts;
}

// This function returns the number of pages that have been read from disk since a buffer pool has been initialized.
extern int getNumReadIO (BM_BufferPool *const bm)
{
	// Adding one because with start rearIndex with 0.
	return (rearIndex + 1);
}

// This function returns the number of pages written to the page file since the buffer pool has been initialized.
extern int getNumWriteIO (BM_BufferPool *const bm)
{
	return writeCount;
}