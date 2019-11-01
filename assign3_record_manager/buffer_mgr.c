#include <stdio.h>
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include <stdlib.h>
#include <sys/stat.h>

//This tells us the size of the buffer pool, the number of page frames that can be contained in the buffer pool.
int buffer_pool_size = 0;

int pages_read = 0;
int page_hits =0;
int WC=0;

// Keep track if buffer is init or shutdown
bool buffer_active = false;

//The actual structure of the page frame inside the buffer pool
typedef struct pg_frame
{
    SM_PageHandle data;
    PageNumber pageNum;
    int Dirty_bit;
    int fix_count;
    int no_of_hits;
} pg_frame;


//FIFO
void FIFO(BM_BufferPool *const bm, pg_frame *page)
{
	pg_frame *pageFrame = (pg_frame *) bm->mgmtData;
	
	int i = 0;
	int front_id;
	front_id = pages_read % buffer_pool_size;

	while(i < buffer_pool_size)
	{	//Check if Page frame being used by some client
		if(pageFrame[front_id].fix_count == 0)
		{
			//  We need to check if that page is modified.
    		// If yes then we first need to write that page back and then replace the page.
			if(pageFrame[front_id].Dirty_bit == 1)
			{
				SM_FileHandle fh;
				//Opening the page file 
				openPageFile(bm->pageFile, &fh);
				//Writing the data to disk
				writeBlock(pageFrame[front_id].pageNum, &fh, pageFrame[front_id].data);
				
				// To keep a count of number of write operations (Statistics functions)
				WC = WC + 1;
			}
			
			// Replacing the values to new one
			pageFrame[front_id].data = page->data;
			pageFrame[front_id].pageNum = page->pageNum;
			pageFrame[front_id].Dirty_bit = page->Dirty_bit;
			pageFrame[front_id].fix_count = page->fix_count;
			break;
		}
		else
		{
			// Page frame being used by some client as fix cpunt is not 0, hence we go to next page frame
			front_id = front_id+1;
			if(front_id % buffer_pool_size == 0)
				front_id = 0;
			else
				front_id = front_id;
		}
		i++;
	}
}


//LRU strategy page replacement strategy
RC LRU(BM_BufferPool *const bm,pg_frame *Page)
{
    int i=0;
	pg_frame *frame = (pg_frame*)bm->mgmtData;
    int least_no_of_hits = 0;
    int LRU_ind =0 ;

    while(i < buffer_pool_size)
	{   
        //If fix_count is not 0, that indicates the page frame is being used by some client and we cant replace it
		if(frame[i].fix_count != 0)
		{
			continue;
		}
        //Else fix_count is 0 and we can replace the page
        else
        {   //initialize the index and least_no_of_hits for least recently used page
            LRU_ind = i;
		    least_no_of_hits = frame[i].no_of_hits;
			break;
        }
        i=i+1;
	}	
    //From all the page frames that can be replaced we find the page frame which can be replaced, that is the one with least no of hits
    for(i = LRU_ind + 1; i < buffer_pool_size; i++)
	{   //Update the LRU_ind and least_no_of_hits
		if(frame[i].no_of_hits < least_no_of_hits)
		{
			LRU_ind = i;
			least_no_of_hits = frame[i].no_of_hits;
		}
	}

    //Now we have the page that can be replaced, but we need to check if that page is modified.
    // If yes then we first need to write that page back and then replace the page.

	//Check if page is dirty (modified)
    if(frame[LRU_ind].Dirty_bit == 1)
	{
		SM_FileHandle *fHandle;
		//Opening the page file 
        openPageFile(bm->pageFile, fHandle);
        //Writing the data to disk
        writeBlock(frame[LRU_ind].pageNum, fHandle, frame[LRU_ind].data);
        //As the page file has been written successfully, it is no longer dirty.
        frame[i].Dirty_bit = 0;
		WC = WC + 1;
	}

    //Replace the page
    frame[LRU_ind].data = Page->data;
	frame[LRU_ind].pageNum = Page->pageNum;
	frame[LRU_ind].Dirty_bit = Page->Dirty_bit;
	frame[LRU_ind].fix_count = Page->fix_count;
	frame[LRU_ind].no_of_hits = Page->no_of_hits;

}

// Buffer Manager Interface Pool Handling
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData)
{
	struct stat buffer;   
  	if (!fopen(pageFileName, "r")){
		//   printf("File not exist\n");
		  return RC_FILE_NOT_FOUND;
	  }

	buffer_active = true;
    //Initializing the values to the respective values passed
    bm->pageFile = (char *)pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;
    //number of pages contained inside the buffer pool
    buffer_pool_size = numPages;
    //Setting the size of Pages (which is of type pg_frame).
    //It will be equal to size of structure (pg_frame) * numPages
    pg_frame *Pages = (pg_frame *)calloc(numPages, sizeof(pg_frame));
    //Storing a pointer to the area in memory that stores the page frames
    bm->mgmtData = Pages;
	WC = 0;

    // printf("here -> %d\n", buffer_pool_size);
    //Initialization for each page in the buffer pool
    for (int i = 0; i < buffer_pool_size; i++)
    { 
		//Initially all pages should be empty hence data = NULL
        Pages[i].data = NULL;
        //Initially no. of pages
        Pages[i].pageNum = -1;
        //No modifications to any pages hence dirty bit is 0
        Pages[i].Dirty_bit = 0;
        //No client has pinned any pages yet hence fix count is 0
        Pages[i].fix_count = 0;
		//Initialize no_of_hits to 0
        Pages[i].no_of_hits = 0;
        // printf("pages in init -> %d\n", Pages[i].fix_count);
    }
    return RC_OK;
}

//To free up all resources associated with buffer pool
RC shutdownBufferPool(BM_BufferPool *const bm)
{

	if(!buffer_active)
		return RC_UNABLE_TO_REMOVE;
    int i = 0;
    pg_frame *frames = (pg_frame *)bm->mgmtData;
    // printf("here -> %d\n", buffer_pool_size);

    forceFlushPool(bm);
    //Check if there are any pinned pages in the buffer
    while (i < buffer_pool_size)
    {
        // printf("pages in shut for %d-> %d\n",i,frames[i].fix_count);
        //if the fix count value = 0 then there is no client using that page (hence these pages can be removed from the buffer pool)
        if (frames[i].fix_count == 0)
        {
            printf("No pinned pages in the frame %d so we can free the resources\n", i);
        }
        else
        {
            printf("There are pinned pages in the frame %d, hence we cannot free the resources\n", i);
            return RC_UNABLE_TO_REMOVE;
        }
        i++;
    }
    // If there are no pinned pages in any of the frames we can free them up
    free(frames);
    //As we freed our frames the pointer should be now initialized to NULL as the frame does not exist
    bm->mgmtData = NULL;
	buffer_active = false;
    return RC_OK;
}
//all dirty pages (with fix count 0) from the buffer pool to be written to disk
RC forceFlushPool(BM_BufferPool *const bm)
{
	if(!buffer_active)
		return RC_UNABLE_TO_REMOVE;
    SM_FileHandle fHandle;
    int i = 0;
    pg_frame *frames = (pg_frame *)bm->mgmtData;
    while (i < buffer_pool_size)
    { //if the fix count value = 0 then there is no client using that page
        if (frames[i].fix_count == 0)
        {
            // If the dirty bit = 1 then we need to open the page file and write the contents to the page file on disk
            if (frames[i].Dirty_bit == 1)
            {
				// Checking if page is in memory
                //Opening the page file
                RC a = openPageFile(bm->pageFile, &fHandle);
                //Writing the data to disk
                writeBlock(frames[i].pageNum, &fHandle, frames[i].data);
                //As the page file has been return successfully, it is no longer dirty.
                frames[i].Dirty_bit = 0;
				WC = WC + 1;
				closePageFile(&fHandle);
            }
        }
        i++;
    }
    return RC_OK;
}

RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page,
           const PageNumber pageNum)
{
	if(!buffer_active)
		return RC_UNABLE_TO_REMOVE;
	if(pageNum < 0){
	return RC_ERROR_UNABLE_FORCING_PAGE;
	}

	pg_frame *frames = (pg_frame *)bm->mgmtData;
    pg_frame *pool_frame = (pg_frame *) malloc(sizeof(pg_frame));
	
	// If there are no pages in the buffer pool , then we pin the first page to pool
	if(frames[0].pageNum == -1)
	{
		// initializing of page of buffer pool
		pages_read = 0;
        page_hits= 0;
		SM_FileHandle fh;
		openPageFile(bm->pageFile, &fh);
		frames[0].data = (SM_PageHandle) calloc(1,PAGE_SIZE);
		ensureCapacity(pageNum,&fh);
		readBlock(pageNum, &fh, frames[0].data);
		frames[0].pageNum = pageNum;
		frames[0].fix_count = frames[0].fix_count +1;
		frames[0].no_of_hits = 0;	
		page->pageNum = pageNum;
		page->data = frames[0].data;
		closePageFile(&fh);
		
		return RC_OK;		
	}
	else
	{	
		bool BF = true;
		int i=0;
		
		while( i < buffer_pool_size)
		{
			if(frames[i].pageNum != -1)
			{	
				if(frames[i].pageNum == pageNum)
				{
				// Checking if page is in memory
					// printf("Page in mem\n");
					// Incrementing fix count, as the page is being accessed by an additional client
					frames[i].fix_count ++;
					BF = false;
					page_hits = page_hits + 1; //Keep count of hits

					if(bm->strategy == RS_LRU)
						// Update no of hits for that page for LRU	
						frames[i].no_of_hits = page_hits;
					
					page->pageNum = pageNum;
					page->data = frames[i].data;
					break;
				}

			} 
			else 
			{
				// printf("Here\n");
				SM_FileHandle fh;
				//Open Page File
				openPageFile(bm->pageFile, &fh);
				frames[i].data = (SM_PageHandle) calloc(1,PAGE_SIZE);
				//Read Page file
				readBlock(pageNum, &fh, frames[i].data);
				pages_read = pages_read +1;	// no of read occured for staticstics function
				page_hits = page_hits + 1; //Keep count of hits
				//Set pageNum and fix_count
				frames[i].pageNum = pageNum;
				frames[i].fix_count = 1;

				if(bm->strategy == RS_LRU)
					// Update no of hits for that page for LRU
					frames[i].no_of_hits = page_hits;				
				page->pageNum = pageNum;
				page->data = frames[i].data;
				
				BF= false;

				break;
			}
			i=i+1;
		}
		
		// We must choose a replacement strategy to replace the page if the buffer is full
		if(BF == true)
		{
			// printf("Buffer full for page num %i\n", pageNum);

			int pinned_frames = 0;

			for(int i =0; i< buffer_pool_size;i++){
				if(frames[i].fix_count>0){
					pinned_frames++;
				}
			}
			if(pinned_frames==buffer_pool_size){
				return RC_ERROR_UNABLE_TO_UNPIN_PAGE;
			}
			// Updating the buffer pool
			SM_FileHandle fh;
			//Open Page File
			openPageFile(bm->pageFile, &fh);
			pool_frame->data = (SM_PageHandle) calloc(1,PAGE_SIZE);
			//Read Page file
			readBlock(pageNum, &fh, pool_frame->data);
			pages_read = pages_read +1;	// no of read occured for staticstics function
			page_hits = page_hits + 1; //Keep count of hits
			pool_frame->pageNum = pageNum;
			pool_frame->Dirty_bit = 0;		
			pool_frame->fix_count = 1;
			//Close Page file
			closePageFile(&fh);
			

			if(bm->strategy == RS_LRU)
				// Update no of hits for that page for LRU
				pool_frame->no_of_hits = page_hits;				
			page->pageNum = pageNum;
			page->data = pool_frame->data;			

			// Choose the replacement strategy to be used
			switch(bm->strategy)
			{	// FIFO Implementation		
				case RS_FIFO: 
					FIFO(bm, pool_frame);
					break;
				// LRU implementation
				case RS_LRU: 
					LRU(bm, pool_frame);
					break;
				
				default:
					printf("\nStrategy not available ");
					break;
			}
						
		}		
		return RC_OK;
	}	
}



//The buffer manager needs to know whether the page was modified by the client, If yes then we mark that page as dirty
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    int i=0;
	pg_frame *frame = (pg_frame*)bm->mgmtData;
    while(i < buffer_pool_size)
    {   //Check whether the current page (pg_frame[i].pageNum) is the one that needs to be marked dirty 
        if(frame[i].pageNum == page->pageNum)
        {   // If yes we mark that page as dirty by setting its Dirty_bit to 1
            frame[i].Dirty_bit = 1;
            return RC_OK;       
        }
        i = i+1;           
    }  
    //If we dont find the page in buffer pool then we send an error      
    return RC_ERROR_UNABLE_TO_MAKE_PAGE_DIRTY;
}

//When the client no longer needs the page we need to unpin the page (remove the page from buffer pool)
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    int i=0;
	pg_frame *frame = (pg_frame*)bm->mgmtData;
    while(i < buffer_pool_size)
    {   //Check whether the current page (pg_frame[i].pageNum) is the one that needs to be unpinned
        if(frame[i].pageNum == page->pageNum)
        {       // If yes then we reduce its fix count by 1 (It indicates that the page is not need by this client)
                frame[i].fix_count = frame[i].fix_count - 1;
				//We come out of the loop 
                return RC_OK;
        }
        i = i+1; 
    }
    //If we dont find the page in buffer pool then we send an error 
    return RC_ERROR_UNABLE_TO_UNPIN_PAGE;

}

//The dirty pages (modified pages) should be written back to the page file on disk
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{   
    SM_FileHandle fHandle;
    int i=0;
	pg_frame *frame = (pg_frame*)bm->mgmtData;
    while(i < buffer_pool_size)
    {//Check whether the current page (pg_frame[i].pageNum) is the one that needs to be written back
        if(frame[i].pageNum == page->pageNum)
        {   //If yes we open the page file
			// printf("Here!! \n");
            openPageFile(bm->pageFile, &fHandle);
            //Writing the data to disk
            writeBlock(frame[i].pageNum, &fHandle, frame[i].data);
            //As the page file has been written successfully, it is no longer dirty.
            frame[i].Dirty_bit = 0;
			WC = WC + 1;
            return RC_OK;
        }
        i = i+1; 
    }
    return RC_ERROR_UNABLE_FORCING_PAGE;
}

// The getFrameContents function returns an array of PageNumbers (of size numPages) where the ith element is the number of the page stored in the ith page frame
PageNumber *getFrameContents (BM_BufferPool *const bm)
{	
	int i=0;
	pg_frame *frame = (pg_frame*)bm->mgmtData;
	PageNumber *FCon = calloc(1,sizeof(PageNumber) * buffer_pool_size);
	
	while(i < buffer_pool_size) 
	{	
		if(frame[i].pageNum == -1){
			FCon[i] = NO_PAGE;
		}
		else {
			FCon[i] = frame[i].pageNum;
			// printf("FCon[i] -> %d\n",frame[i].pageNum);
		}
		i++;
	}
	// printf("Fcon -> %i\n",FCon);
	return FCon;
}

// The getDirtyFlags function returns an array of bools (of size numPages) where the ith element is TRUE if the page stored in the ith page frame is dirty.
bool *getDirtyFlags (BM_BufferPool *const bm)
{	
	int i=0;
	pg_frame *frame = (pg_frame*)bm->mgmtData;
	bool *DF = calloc(1,sizeof(bool) * buffer_pool_size);

	while(i < buffer_pool_size) 
	{	
		if(frame[i].Dirty_bit == 1)
			DF[i] = TRUE;
		else
			DF[i] = FALSE;
		i++;
	}
	return DF;
}

// The getFixCounts function returns an array of ints (of size numPages) where the ith element is the fix count of the page stored in the ith page frame. 
int *getFixCounts (BM_BufferPool *const bm)
{
	int i=0;
	pg_frame *frame = (pg_frame*)bm->mgmtData;
	int *FC = calloc(1,sizeof(int) * buffer_pool_size);

	while(i < buffer_pool_size) 
	{	
		if(frame[i].fix_count == -1){
			FC[i] = 0;
		}
		else{
			FC[i] = frame[i].fix_count;
			// printf("FC[i] -> %d\n",frame[i].fix_count);

		}
		i++;
	}
	return FC;
}

//The getNumReadIO function returns the number of pages that have been read from disk since a buffer pool has been initialized.
int getNumReadIO (BM_BufferPool *const bm)
{
	return (pages_read + 1);
}

// The getNumWriteIO returns the number of pages written to the page file since the buffer pool has been initialized
int getNumWriteIO (BM_BufferPool *const bm)
{
	return WC;
}