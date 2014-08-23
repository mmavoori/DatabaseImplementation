#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

#include<iostream>
#include<stdlib.h>
#include<string.h>

////////////////////////////// GenericDBFile ///////////////////////////////////

int GenericDBFile::GetFileLength()
{
    if(file.GetLength() == 0)
        return file.GetLength();
    else
        return file.GetLength() - 1;
}

GenericDBFile::GenericDBFile() 
{
    // create an empty read buffer
    readPageBuffer = new (std::nothrow) Page;
    if(readPageBuffer == NULL)
    {
        cout << "ERROR : Not enough memory. EXIT !!!\n";
		exit(1);
    }
    
    // create an empty write buffer
    writePageBuffer = new (std::nothrow) Page;
    if(writePageBuffer == NULL)
    {
        cout << "ERROR : Not enough memory. EXIT !!!\n";
		exit(1);
    }
}

////////////////////////////// HeapDBFile //////////////////////////////////////

// Algorithm used in filling and commiting the write buffer: (this handles all
// combinations of Add and GetNext so that there are no gaps in the pages of the file)
//
// The flag 'writeBufferCopyOfLastPage' tells if the write buffer is a copy of 
// the file's last page or a fresh buffer.
//
// GetPage() into write buffer:
//         case1: with last page of file --> set writeBufferCopyOfLastPage = 1
//         case2: a fresh buffer         --> set writeBufferCopyOfLastPage = 0
//
// AddPage() from write buffer:
//         if(writeBufferCopyOfLastPage)
//                commit to last existing page of file by overwriting it
//         else
//                commit to a fresh page after the existing last page of file

HeapDBFile::HeapDBFile() : GenericDBFile()
{ 
    // Call super class constructor and...
    writeBufferDirty = 0;
    writeBufferCopyOfLastPage = 0;
    curReadPageIndex = -2; // file.GetLength() starts at 0 and takes values 2,3,4... & not 1
                           // Actually it should start at 1 and take values 2,3,4... 
                           // (Confusing@#!$* bcoz 'O'th page has no data in a file)
}

int HeapDBFile::Open (char *file_path) 
{
    file.Open(1,file_path);
    
    // Get the first page of the file, if any, into the read buffer
    if(GetFileLength() > 0) {   // means not an empty file
         curReadPageIndex = 0;
         file.GetPage(readPageBuffer, curReadPageIndex);
         recordIndex = 0;
    }
    
    // Get the last page of the file, if any, into the write buffer, because
    // we have to append records to the file without leaving any spaces.
    if(GetFileLength() > 0) {   // means not an empty file  
         file.GetPage(writePageBuffer, GetFileLength() - 1);
         writeBufferDirty = 0;
         writeBufferCopyOfLastPage = 1;
    }
    
    return 1;
}

void HeapDBFile::MoveFirst() 
{
     // Get the first page of the file into the read buffer
     readPageBuffer->EmptyItOut();
     file.GetPage(readPageBuffer,0);
     recordIndex = 0;
     curReadPageIndex = 0;
}

int HeapDBFile::Close() 
{
    // write to disk any unwritten buffer
    // Almost always write buffer is dirty, except in the cases where you commit
    // non-full write buffer and where you close the 
    // file without writing even a single record to the buffer
    if(writeBufferDirty) {
         if(writeBufferCopyOfLastPage == 1) {
              file.AddPage(writePageBuffer, GetFileLength() - 1);
          }
          else { 
               // Freshwrite allOtherCases: commit to a fresh page after last page.
              file.AddPage(writePageBuffer, GetFileLength());
          }
          
          writePageBuffer->EmptyItOut();
          writeBufferDirty = 0;
          writeBufferCopyOfLastPage = 0;
    }
    
    int file_pageLength = file.Close();
    delete readPageBuffer;
    delete writePageBuffer;
    return file_pageLength;
}


void HeapDBFile::Load (Schema &f_schema, char *load_file_path) 
{
     FILE *file = fopen(load_file_path,"r");
     if(file == NULL) 
     {
             cerr<< "Opening file \""<< load_file_path <<"\" failed.\n";
             exit(1);
     }
     
     // Suck each record from the text file and add it to write buffer
     Record temp;

     int counter = 0;
	 while (temp.SuckNextRecord(&f_schema, file)) {
		Add(temp);
        counter++;
	 }
     cout<<"Loaded " << counter << " records from the text file.\n";
	 
	 fclose(file);
}

// First add the record to the write buffer.
// If the write buffer is full, write it to the disk, empty it out and
// then add the record to it.
void HeapDBFile::Add (Record &addme) 
{
     // add the record to the write buffer
     int appended = writePageBuffer->Append(&addme);
     
     if(!appended) {
          // Write buffer is full, so write it to the disk
          
          // Write buffer overwrite case1: 
          // if writeBufferCopyOfLastPage, we have to overwrite to the last page of the file
          // else commit it to a fresh page after the last page
          // GetFileLength() - 1 or file.GetLength - 2 gives the index of the last page of the file
          if(writeBufferCopyOfLastPage == 1) {
              file.AddPage(writePageBuffer, GetFileLength() - 1);
          }
          else { 
               // Freshwrite allOtherCases: commit to a fresh page after last page.
              file.AddPage(writePageBuffer, GetFileLength());
          }
          
          // now add the record to a fresh buffer
          writePageBuffer->EmptyItOut();
          writeBufferDirty = 0;
          writeBufferCopyOfLastPage = 0;
          
          appended = writePageBuffer->Append(&addme);
     }
     
     writeBufferDirty = 1;
}

// Fetch the next record from the read buffer.
// IF the read buffer is empty, then fill it with the next page from the disk
// and then fetch the record from it.
int HeapDBFile::GetNext (Record &fetchme) 
{
    // Logic for mixed read and writes from the disk's last page:
    // Here write buffer is not full, but we write it to the disk
    //
    // Write buffer overwrite case2: If we are reading from the last page of the file, and
    // if the write buffer is dirty: 
    // a. if writeBufferCopyOfLastPage, overwrite the dirty write buffer to the disk's last page,
    //    and read back the last page and skip all fetched records
    // b. if writeBuffer Not CopyOfLastPage, do nothing (Means write buffer has some new
    //    page. We'll handle this case in the coming if statement)
    if((curReadPageIndex == GetFileLength() - 1) && writeBufferDirty == 1) {
                         
          if(writeBufferCopyOfLastPage == 1) {
              // Overwrite the disk's last page
              file.AddPage(writePageBuffer, GetFileLength() - 1);
              
              // last page is a copy of the write buffer, which means effectively
              // still write buffer is a copy of the last page
              // Doing this bcoz write buffer is not full, but we are writing it to the disk
              writeBufferCopyOfLastPage = 1;
              writeBufferDirty = 0;
              
              // read back overwritten last page
              readPageBuffer->EmptyItOut();
              file.GetPage(readPageBuffer, GetFileLength() - 1);
              curReadPageIndex == GetFileLength() - 1;
              
              // skip all fetched records
              Record temp;
              for (int i = 0; i < recordIndex; i++) {
    			readPageBuffer->GetFirst(&temp);
    		  }
        }
    }
          
    // get the next record from the read buffer
    int got_next = readPageBuffer->GetFirst(&fetchme);
    
    if(!got_next) {
          // read buffer is empty
          
          // Check if this is the last page of the file
          //cout<< "curReadPageIndex : "<< curReadPageIndex <<endl ;
          if(curReadPageIndex == (GetFileLength() - 1)) {
               // Check if there is any uncommited dirty write buffer 
               // If yes, write it to a fresh page after last page and load the read buffer and continue reading
               // otherwise finished reading the entire file
               if(writeBufferDirty) {
                      file.AddPage(writePageBuffer, GetFileLength());
                      
                      // last page is a copy of the write buffer, which means effectively
                      // still write buffer is a copy of the last page
                      // Doing this bcoz write buffer is not full, but we are writing it to the disk
                      writeBufferCopyOfLastPage = 1;
                      writeBufferDirty = 0;
                }
                else {
                     return 0; // finished reading the entire file
                }
          }
               
          // read next page from the disk
          readPageBuffer->EmptyItOut();
          file.GetPage(readPageBuffer, ++curReadPageIndex);
          recordIndex = 0;
             
          // now get the next record from the read buffer
          got_next = readPageBuffer->GetFirst(&fetchme);
    }
    
    recordIndex++;
    return 1;
}

int HeapDBFile::GetNext (Record &fetchme, CNF &applyMe, Record &literal) 
{
    ComparisonEngine comparisionEngine;
	
	// Get each record and compare
	do {
		if (GetNext(fetchme) == 0) {
			return 0;  // finished reading the entire file
		}
	} while (!comparisionEngine.Compare(&fetchme, &literal, &applyMe));
	
	return 1;
}

////////////////////////////// SortedDBFile ////////////////////////////////////

SortedDBFile::SortedDBFile (void *startup, ofstream& fout) : GenericDBFile()
{
    // Call super class constructor and then write the sortorder to the metadata file
    sortInfo = (SortInfo *) startup;
    sortInfo->myOrder->WriteToFile(fout);
    fout << sortInfo->runLength << endl;
    
    curReadPageIndex = -2;
    searchPage = NULL;
	searchOrderMaker = NULL;
	literalOrderMaker = NULL;
    
    // Set initial mode to 'reading'
    mode = 'r';
}

SortedDBFile::SortedDBFile (ifstream &fin) : GenericDBFile()
{
    // Call super class constructor and then read the sortorder from the metadata file
    sortInfo = new SortInfo();
    sortInfo->myOrder = new OrderMaker(fin);
    fin >> sortInfo->runLength;
    
    curReadPageIndex = -2;
    searchPage = NULL;
	searchOrderMaker = NULL;
	literalOrderMaker = NULL;
    
    // Set initial mode to 'reading'
    mode = 'r';
}

int SortedDBFile::Open (char *file_path) 
{
    file.Open(1,file_path);
    
    // Get the first page of the file, if any, into the read buffer
    if(GetFileLength() > 0) {   // means not an empty file
         curReadPageIndex = 0;
         file.GetPage(readPageBuffer, curReadPageIndex);
    }
    
    // Set initial mode to 'reading'
    mode = 'r';
    
    return 1;
}

void SortedDBFile::MoveFirst() 
{
     // Get the first page of the file into the read buffer
     readPageBuffer->EmptyItOut();
     file.GetPage(readPageBuffer,0);
     curReadPageIndex = 0;
}

int SortedDBFile::Close() 
{
    // Closing while in 'loading' mode
    if(mode == 'l') {
        // Write to disk any half-full unwritten buffer
        // Almost always write buffer is half-full, except in the case where the records
        // fit exactly into completely full pages.
       
        // Commit to a fresh page after last page.
        file.AddPage(writePageBuffer, GetFileLength());
        writePageBuffer->EmptyItOut();
    }
    
    // Closing while in 'writing' mode
    if(mode == 'w') {
         // Get the recently added records (if any) from the bigQ
         // and add them to the SortedDBFile using MergeSort
         
         // First shutdown the toBigQPipe opened in Add(Record &addme)
         toBigQPipe->ShutDown();
         
         // By now BigQ would have formed the runs, sorted each run and written them to a temp file.
	     // Now it'll start its 2nd phase of tpmms which is merging all the runs and putting
	     // the sorted records in the 'fromBigQPipe'.
	 
    	 // So start a consumer thread that consumes the sorted records from the 'fromBigQPipe'
    	 // and adds them to the SortedDBFile using MergeSort
    	 pthread_t thread3;
    	 
    	 if(GetFileLength() == 0) {
              // SortedDBFile is empty
              pthread_create (&thread3, NULL, collectFromBigQ, (void *)this);
         }
         else {
              // SortedDBFile is not empty, so merge
              pthread_create (&thread3, NULL, mergeToSortedDBFile, (void *)this);
         }
    	 
    	 pthread_join (thread3, NULL); // wait till the thread completes
    	 
    	 // Delete the pipe and bigQ instances
    	 delete toBigQPipe;
    	 delete fromBigQPipe;
    	 delete bigQ;
    }
    
    // For all modes = 'r' / 'm' / 'w' / 'l'
    int file_pageLength = file.Close();
    delete readPageBuffer;
    delete writePageBuffer;
    
    return file_pageLength;
}

void SortedDBFile::Load (Schema &f_schema, char *load_file_path) 
{
     // Set mode to 'loading'
     mode = 'l';
     
     FILE *file = fopen(load_file_path,"r");
     if(file == NULL) 
     {
             cerr<< "Opening file \""<< load_file_path <<"\" failed.\n";
             exit(1);
     }
     
     // Create pipes
     toBigQPipe = new Pipe(100);
     fromBigQPipe = new Pipe(100);
     
     // Create a BigQ instance. This automatically starts a thread here.
     bigQ = new BigQ(*toBigQPipe, *fromBigQPipe, *(sortInfo->myOrder), sortInfo->runLength);
     
     // Suck each record from the text file and add it to the 'toBigQPipe'
     Record temp;
	 while (temp.SuckNextRecord(&f_schema, file)) {
		toBigQPipe->Insert(&temp);
	 }
	 // Signal BigQ that no more data will be inserted
	 toBigQPipe->ShutDown();
	 
	 // By now BigQ would have formed the runs, sorted each run and written them to a temp file.
	 // Now it'll start its 2nd phase of tpmms which is merging all the runs and putting
	 // the sorted records in the 'fromBigQPipe'.
	 
     // So start a consumer thread that consumes the sorted records from the 'fromBigQPipe'
	 // and writes them to the writePageBuffer
	 pthread_t thread2;
	 pthread_create (&thread2, NULL, collectFromBigQ, (void *)this);
	 
	 pthread_join (thread2, NULL); // wait till the thread completes
	 
	 delete toBigQPipe;
	 delete fromBigQPipe;
	 delete bigQ;
	 fclose(file);
}

void *collectFromBigQ(void *arg)
{
     SortedDBFile *sortedDBFile = (SortedDBFile *) arg;
     
     // Get each sorted record from 'fromBigQPipe' and add it to write buffer
     Record temp;
     int counter = 0;
	 while ( sortedDBFile->fromBigQPipe->Remove(&temp) ) {
		// Directly add to writePageBuffer. Not calling Add(Record &addme) here.
        sortedDBFile->AddToWriteBuffer(temp);
		counter++;
	 }
	 
	 // Always write the last half-full buffer
	 sortedDBFile->file.AddPage(sortedDBFile->writePageBuffer, sortedDBFile->GetFileLength());
     sortedDBFile->writePageBuffer->EmptyItOut();
          
	 // Shutdown output pipe.
	 sortedDBFile->fromBigQPipe->ShutDown();
}

// Same functionality as heap file's Add(Record &addme)
void SortedDBFile::AddToWriteBuffer (Record &addme) 
{
     // add the record to the write buffer
     int appended = writePageBuffer->Append(&addme);
     
     if(!appended) {
          // Write buffer is full, so write it to the disk
          
          // Commit it to a fresh page after the last page
          file.AddPage(writePageBuffer, GetFileLength());
          
          // now add the record to a fresh buffer
          writePageBuffer->EmptyItOut();
          appended = writePageBuffer->Append(&addme);
     }
}

// Calling this function means SortedDBFile should be changed to "writing" mode.
// Keep adding records to bigQ till read {GetNext()} is called. 
void SortedDBFile::Add(Record &addme)
{
     // While changing from "reading" mode to "writing" mode
     if(mode == 'r') {
         // Create pipes
         toBigQPipe = new Pipe(100);
         fromBigQPipe = new Pipe(100);
     
         // Create a BigQ instance. This automatically starts a thread here.
         bigQ = new BigQ(*toBigQPipe, *fromBigQPipe, *(sortInfo->myOrder), sortInfo->runLength);   
             
         mode = 'w'; // Change to writing mode
     }
     
     toBigQPipe->Insert(&addme);
}

int SortedDBFile::GetNext (Record &fetchme) 
{
    // While changing from "writing" mode to "reading" mode
     if(mode == 'w') {
         // Get the recently added records (if any) from the bigQ
         // and add them to the SortedDBFile using MergeSort
         
         // First shutdown the toBigQPipe opened in Add(Record &addme)
         toBigQPipe->ShutDown();
         
         // By now BigQ would have formed the runs, sorted each run and written them to a temp file.
	     // Now it'll start its 2nd phase of tpmms which is merging all the runs and putting
	     // the sorted records in the 'fromBigQPipe'.
	 
    	 // So start a consumer thread that consumes the sorted records from the 'fromBigQPipe'
    	 // and adds them to the SortedDBFile using MergeSort
    	 pthread_t thread3;
    	 pthread_create (&thread3, NULL, mergeToSortedDBFile, (void *)this);
    	 
    	 pthread_join (thread3, NULL); // wait till the thread completes
    	 
    	 // Delete the pipe and bigQ instances
    	 delete toBigQPipe;
    	 delete fromBigQPipe;
    	 delete bigQ;
	 
         mode = 'r'; // Change to reading mode
     }
          
    // get the next record from the read buffer
    int got_next = readPageBuffer->GetFirst(&fetchme);
    
    if(!got_next) {
          // read buffer is empty
          
          // Check if this is the last page of the file
          if(curReadPageIndex == (GetFileLength() - 1)) {
               return 0; // finished reading the entire file
          }
               
          // read next page from the disk
          readPageBuffer->EmptyItOut();
          file.GetPage(readPageBuffer, ++curReadPageIndex);
             
          // now get the next record from the read buffer
          got_next = readPageBuffer->GetFirst(&fetchme);
    }
    
    return 1;
}

// Get the recently added records (if any) from the bigQ and add them to the 
// SortedDBFile using MergeSort.
void *mergeToSortedDBFile(void *arg)
{
    SortedDBFile *sortedDBFile = (SortedDBFile *) arg;	 
    sortedDBFile->mode = 'm';  // Changing the mode to 'merging'
    
    ComparisonEngine ce;
	
	// Create a merge file that contains the results of the mergeSort of the current
	// SortedDBFile and th enewly added records in the bigQ.
	char mergeFileName[100];
	sprintf(mergeFileName, "%s_MergeFile%d", sortedDBFile->fileName, getppid());
	File mergeFile;
	mergeFile.Open(0, mergeFileName);

    // Holds pages from the current SortedDBFile.
    Page mergeReadPageBuffer;
	int readPageIndex = 0; // Till which page of current SortedDBfile is it merged.
  
    // Holds merged results of the current SortedDBFile and newly added records
    // that are waiting to be written to the mergeFile.
    Page mergeWritePageBuffer;
    int mergeFilePageIndex = 0; // Till which page is mergeFile written.
    
	// Get the first page of the current SortedDBFile into mergeReadPageBuffer.
	bool fileHasMore = 0;
	Record curSortedDBFileRecord;
	if (sortedDBFile->GetFileLength() > 0) {   // means not an empty file
		sortedDBFile->file.GetPage(&mergeReadPageBuffer, readPageIndex);
		fileHasMore = mergeReadPageBuffer.GetFirst(&curSortedDBFileRecord);
	}
	
	// Get first newly added record from the bigQ
	Record newlyAddedRecord;
	bool pipeHasMore = sortedDBFile->fromBigQPipe->Remove(&newlyAddedRecord);

	// Loop as long as there is an additional record in the file and/or the pipe.
	while (fileHasMore || pipeHasMore) {
          
        if (fileHasMore && !pipeHasMore) {
           // No comparisions needed. Add all the remaining records in the 
           // current SortedDBFile to the mergeFile and break.
           while(fileHasMore) {
    			int appended = mergeWritePageBuffer.Append(&curSortedDBFileRecord);
    			
                // If mergeWritePageBuffer is full, write it to disk i.e., append to
                // mergeFile.
    			if (!appended) {
    				mergeFile.AddPage(&mergeWritePageBuffer, mergeFilePageIndex++);
    				mergeWritePageBuffer.EmptyItOut();
    				mergeWritePageBuffer.Append(&curSortedDBFileRecord);
    			}
    			
    			// Get the next record from the current SortedDBFile i.e, 
                // from the mergeReadPageBuffer.
    			int got_next = mergeReadPageBuffer.GetFirst(&curSortedDBFileRecord);
    			
                // If mergeReadPageBuffer is empty, get next page from the current SortedDBFile.
    			if (!got_next) {
    				readPageIndex++;
    				
    				if ( readPageIndex == sortedDBFile->GetFileLength() ) {
    				     fileHasMore = 0; // Finished reading all the records from the current SortedDBFile.
    				} 
                    else {
                         sortedDBFile->file.GetPage(&mergeReadPageBuffer, readPageIndex);
    					 mergeReadPageBuffer.GetFirst(&curSortedDBFileRecord);
    				}
    			}// end if
            }// end while
            break;
        }//end if
        
        
        if (pipeHasMore && !fileHasMore) {
           // No comparisions needed. Add all the remaining newly added records 
           // from the bigQ to the mergeFile and break.
           while(pipeHasMore) {
    			int appended = mergeWritePageBuffer.Append(&newlyAddedRecord);
    			
                // If mergeWritePageBuffer is full, write it to disk i.e., append to
                // mergeFile.
    			if (!appended) {
    				mergeFile.AddPage(&mergeWritePageBuffer, mergeFilePageIndex++);
    				mergeWritePageBuffer.EmptyItOut();
    				mergeWritePageBuffer.Append(&newlyAddedRecord);
    			}
    			
    			// Get next record from bigQ.
    			pipeHasMore = sortedDBFile->fromBigQPipe->Remove(&newlyAddedRecord);
          }
          break;
        }
        
        // At this point, both fileHasMore and pipeHasMore. 
        
        // So compare both the records and add the smallest to the mergeWritePageBuffer 
        // and accordingly get the next record from the current SortedDBFile or bigQ.
		if ( ce.Compare(&curSortedDBFileRecord, &newlyAddedRecord, 
                                   sortedDBFile->sortInfo->myOrder) <= 0) {
            // curSortedDBFileRecord is smaller. Add it to the mergeWritePageBuffer.
			int appended = mergeWritePageBuffer.Append(&curSortedDBFileRecord);
			
            // If mergeWritePageBuffer is full, write it to disk i.e., append to
            // mergeFile.
			if (!appended) {
				mergeFile.AddPage(&mergeWritePageBuffer, mergeFilePageIndex++);
				mergeWritePageBuffer.EmptyItOut();
				mergeWritePageBuffer.Append(&curSortedDBFileRecord);
			}
			
			// Get the next record from the current SortedDBFile i.e, 
            // from the mergeReadPageBuffer.
			int got_next = mergeReadPageBuffer.GetFirst(&curSortedDBFileRecord);
			
            // If mergeReadPageBuffer is empty, get next page from the current SortedDBFile.
			if (!got_next) {
				readPageIndex++;
				
				if ( readPageIndex == sortedDBFile->GetFileLength() ) {
				     fileHasMore = 0; // Finished reading all the records from the current SortedDBFile.
				} 
                else {
                     sortedDBFile->file.GetPage(&mergeReadPageBuffer, readPageIndex);
					 mergeReadPageBuffer.GetFirst(&curSortedDBFileRecord);
				}
			}
		} 
        else {
      		// Newly added record from bigQ is smaller. Add it to the mergeWritePageBuffer.
			int appended = mergeWritePageBuffer.Append(&newlyAddedRecord);
			
            // If mergeWritePageBuffer is full, write it to disk i.e., append to
            // mergeFile.
			if (!appended) {
				mergeFile.AddPage(&mergeWritePageBuffer, mergeFilePageIndex++);
				mergeWritePageBuffer.EmptyItOut();
				mergeWritePageBuffer.Append(&newlyAddedRecord);
			}
			
			// Get next record from bigQ.
			pipeHasMore = sortedDBFile->fromBigQPipe->Remove(&newlyAddedRecord);
		}
	}
	
	// Add the last unwritten half-full page to the mergeFile.
	mergeFile.AddPage(&mergeWritePageBuffer, mergeFilePageIndex++);
	
	// Shutdown output pipe.
	sortedDBFile->fromBigQPipe->ShutDown();
	
	// Replace current SortedDBFile with the new mergeFile.
	mergeFile.Close();
	sortedDBFile->file.Close();
	
	remove(sortedDBFile->fileName);
	rename(mergeFileName, sortedDBFile->fileName);
	
	// Open the file for future operations
	sortedDBFile->file.Open(1, sortedDBFile->fileName);
}

int SortedDBFile::GetNext(Record &fetchMe, CNF &applyMe, Record &literal) 
{
	 // While changing from "writing" mode to "reading" mode
     if(mode == 'w') {
         // Get the recently added records (if any) from the bigQ
         // and add them to the SortedDBFile using MergeSort
         
         // First shutdown the toBigQPipe opened in Add(Record &addme)
         toBigQPipe->ShutDown();
         
         // By now BigQ would have formed the runs, sorted each run and written them to a temp file.
	     // Now it'll start its 2nd phase of tpmms which is merging all the runs and putting
	     // the sorted records in the 'fromBigQPipe'.
	 
    	 // So start a consumer thread that consumes the sorted records from the 'fromBigQPipe'
    	 // and adds them to the SortedDBFile using MergeSort
    	 pthread_t thread4;
    	 pthread_create (&thread4, NULL, mergeToSortedDBFile, (void *)this);
    	 
    	 pthread_join (thread4, NULL); // wait till the thread completes
    	 
    	 // Delete the pipe and bigQ instances
    	 delete toBigQPipe;
    	 delete fromBigQPipe;
    	 delete bigQ;
	 
         mode = 'r'; // Change to reading mode
     }
	
	ComparisonEngine ce;
	
	// Find all single equality checks and start matching to sort order
	// When we can no longer match, that is our sort order
	if (searchPage == NULL) {
		OrderMaker *mySortOrder = sortInfo->myOrder;
		
		if (searchOrderMaker != NULL) {
			delete searchOrderMaker;
		}
		searchOrderMaker = new OrderMaker();
		
		if (literalOrderMaker != NULL) {
			delete literalOrderMaker; 
		}
		literalOrderMaker = new OrderMaker();
		
		// Look at each attribute to see if it is involved in a simple equality check
		for (int i = 0; i < mySortOrder->GetNumOfAtts(); i++) {
			int att = mySortOrder->GetAttribute(i);
            int literalIndex = applyMe.HasSimpleEqualityCheck(att);
			
            if (literalIndex != -1) {
				searchOrderMaker->AddAttribute(att, mySortOrder->GetType(i));
				literalOrderMaker->AddAttribute(literalIndex, mySortOrder->GetType(i));
			} 
            else {
				break;
			}
		}
		
		// Binary search
		off_t begin = 0;
		off_t end = file.GetLength() - 2;
		off_t mid = (begin + end) / 2;
		
        searchPage = new Page();
		
        int compareResult;
		int pageWithEqualRecordFirst = -1;
		
        while (begin <= end) {
			mid = (begin + end) / 2;
			
            // Get the middle page
			file.GetPage(searchPage, mid);
			searchPageIndex = mid;
			
			// Get first record from the middlePage and compare to literal
			searchPage->GetFirst(&fetchMe);
			compareResult = ce.Compare(&fetchMe, searchOrderMaker, &literal, literalOrderMaker);
			
			// If record is less than, the required matching record might be in this page 
            // or in any of the upper half of the pages.
			if (compareResult < 0) {
                
                // First search the searchPage completely
				int gotNextRecord;
				do {
					gotNextRecord = searchPage->GetFirst(&fetchMe);
					compareResult = ce.Compare(&fetchMe, searchOrderMaker, &literal, literalOrderMaker);
				} while (gotNextRecord && (compareResult < 0) );
				
                // If no equal record, return 0
				if (compareResult > 0 && gotNextRecord) {
					return 0;
				}
				
				// First matching record found
				if (compareResult == 0) {
					break;
				}
				
				// Continue searching in the upper half of the pages
				begin = mid + 1;
			} 
            else {
                // If record is greater than or equal to, the required matching record might be in 
                // the lower half of the pages.
                
				// If equal to, this is a matching record. But it might not be the first
                // matching record.
				// So note this record and proceed to look in the earlier pages.
				if (compareResult == 0) {
					pageWithEqualRecordFirst = mid;
				}
				compareResult = 1;
				
				// Continue searching in the lower half of the pages
				end = mid - 1;
			}
		} // end while
		
		// If first matching record is found, do nothing
		// Otherwise, check to see if there was only one record at the beginning of a page
		// In all other cases, there must be no record.
		if (compareResult == 0) {
		} 
        else if (pageWithEqualRecordFirst > -1) {
			file.GetPage(searchPage, mid);
			searchPageIndex = mid;
			searchPage->GetFirst(&fetchMe);
		} 
        else {
			delete searchPage;
			searchPage = NULL;
			return 0;
		}
	} // end: if (searchPage == NULL)
    else {
         // Binary search is already done, so just get the next record from the searchPage
		int gotNextRecord = searchPage->GetFirst(&fetchMe);
		
        if (!gotNextRecord){
            searchPageIndex++;
			if ( searchPageIndex < file.GetLength() - 1 ) {
				file.GetPage(searchPage, searchPageIndex);
				searchPage->GetFirst(&fetchMe);
			} else {
				return 0;
			}
		}
	} // end: else (searchPage == NULL)

	// Continue until we find the next matching record
	while (!ce.Compare(&fetchMe, &literal, &applyMe)) {
		// There will be no more records to return, if the next record is not 
        // in the range of records that matched the order maker.
		if (ce.Compare(&fetchMe, searchOrderMaker, &literal, literalOrderMaker) != 0) {
			return 0;
		}
		
		// Get next record from the searchPage and if there are no more records, get next page
		int gotNextRecord = searchPage->GetFirst(&fetchMe);
		
        if (!gotNextRecord){
            searchPageIndex++;
            if ( searchPageIndex < file.GetLength() - 1 ) {
				file.GetPage(searchPage, searchPageIndex);
				searchPage->GetFirst(&fetchMe);
			} 
            else {
				return 0;
			}
		} // end if
	} // end while
	
	return 1;
}

////////////////////////////// DBFile //////////////////////////////////////////

DBFile::DBFile() 
{ }

int DBFile::Create (char *file_path, fType file_type, void *startup) 
{
    char metaFileName[50];
    sprintf(metaFileName, "%s.metadata", file_path);
    
    ofstream fout;
    fout.open(metaFileName);
    if(fout.fail()) {              
        cout<<"\nOpening " << metaFileName << " file for writing failed.\n";
        exit(1);
    }
    
    if(file_type == heap) {
        fout<< "heap\n";
        myInternalVar = new HeapDBFile();
    }
    else if(file_type == sorted) {
        fout<< "sorted\n";
        myInternalVar = new SortedDBFile(startup, fout);
    }
    else {
        cout<<"\nInvalid file_type specified.\n";
        exit(1);
    }
    
    myInternalVar->fileName = new char[100];
	strcpy(myInternalVar->fileName, file_path);
	
    myInternalVar->file.Open(0,file_path);
    
    fout.close();
    return 1;
}

int DBFile::Open (char *file_path)
{
    char metaFileName[50];
    sprintf(metaFileName, "%s.metadata", file_path);
    
    ifstream fin;
    fin.open(metaFileName);
    if(fin.fail()) {              
        cout<<"\nOpening " << metaFileName << " file for reading failed.\n";
        return 0;
    }
    
    string file_type;
    getline(fin, file_type);
    
    if(file_type == "heap") {
        myInternalVar = new HeapDBFile();
    }
    else if(file_type == "sorted") {
        myInternalVar = new SortedDBFile(fin);
    }
    else {
        cout<<"\nInvalid file_type \"" << file_type << "\" read from " 
            << metaFileName << " file.\n";
        exit(1);
    }
    
    myInternalVar->fileName = new char[100];
	strcpy(myInternalVar->fileName, file_path);
	
    return myInternalVar->Open(file_path);
}

void DBFile::MoveFirst() 
{
    myInternalVar->MoveFirst();
}

int DBFile::Close()
{
	int file_pageLength = myInternalVar->Close();
	delete myInternalVar;
	
	return file_pageLength;
}

void DBFile::Load(Schema &f_schema, char *load_file_path)
{
    myInternalVar->Load(f_schema, load_file_path);
}

void DBFile::Add(Record &addme)
{
    myInternalVar->Add(addme);
}

int DBFile::GetNext(Record &fetchme)
{
    myInternalVar->GetNext(fetchme);
}

int DBFile::GetNext(Record &fetchme, CNF &applyMe, Record &literal) 
{
    myInternalVar->GetNext(fetchme, applyMe, literal);
}
