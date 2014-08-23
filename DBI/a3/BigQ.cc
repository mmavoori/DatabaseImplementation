#include "BigQ.h"
#include<stdlib.h>
#include<algorithm>
#include<queue>
#include<iostream>

// Comparator for sort() and priorityQueue
class Comparator {
private:
	OrderMaker* sortOrder;
	ComparisonEngine ce;
	
public:
	Comparator(OrderMaker* sortOrder) {
       this->sortOrder = sortOrder;
    }
	
    bool operator() (Record *rec1, Record *rec2)
    {
         int result = ce.Compare(rec1, rec2, sortOrder);
         return (result < 0 ? true : false);
    }
    
    bool operator() (PriorityQueueRecord *pqrec1, PriorityQueueRecord *pqrec2)
    {
         int result = ce.Compare(pqrec1->GetRecord(), pqrec2->GetRecord(), sortOrder);
         return (result < 0 ? false : true);
    }
};

BigQ :: BigQ (Pipe &inPipe, Pipe &outPipe, OrderMaker &sortOrder, int runLength) 
{
     this->inPipe    = &inPipe;
     this->outPipe   = &outPipe;
     this->sortOrder = &sortOrder;
     this->runLength = runLength;
     
     pthread_create(&workerThread, NULL, tpmms, (void *) this);
}

// TODO: Split tpmms function into phase1 and phase2 functions
void *tpmms(void *arg)
{
	BigQ *bigQ = (BigQ *) arg;
	
	vector<Record *> run;
	vector<int> runStartPageIndexes; // To keep track of where each run starts in the temp file
	vector<int> runEndPageIndexes;   // To keep track of where each run ends in the temp file
	int tempFilePageIndex = 0;       // Index of the next free page in the temp file
    
    // Create a temporary file to write the runs
    char tempFileName[50];
    sprintf( tempFileName, "BigQ_tempFile_%d", abs((int) bigQ->workerThread) );
    
    File tempFile;
    tempFile.Open(0, tempFileName);
    
    // Phase 1
    // Create a temporary measuringPage to measure the no:of records read from in pipe
    Page *measuringPage = new (std::nothrow) Page;
    if(measuringPage == NULL) {
             cout<<"Not enought memory.\n";
             exit (1);
    }
    
    // Start reading records from the input Pipe, 
    // when a page of records are read from the in pipe add the page to a run, 
    // when the run has runLength of pages sort the run and write it to the temporary file
    Record nextRecord;
    while( bigQ->inPipe->Remove(&nextRecord) ) 
    {      
           int pagesRead = 0;   // No:of pages read from in pipe
           bool runComplete = false;
           
           // Read runLength pages of records from in pipe
           do {
                  // Add the record to the measuringPage 
                 int appended = measuringPage->Append(&nextRecord);
                 
                 // Check if measuringPage is full
                 if( ! appended) {
                     // Transfer measuringPage's contents to run
                     // TODO: Replace tempRecord with runRecord
                     Record *tempRecord = new (std::nothrow) Record;
                     if(tempRecord == NULL) {
                          cout<<"Not enought memory.\n";
                          exit (1);
                     }
        
                     while( measuringPage->GetFirst(tempRecord) ) {
                          // Create space for a new run record
                          Record *runRecord = new (std::nothrow) Record;
                          runRecord->Consume(tempRecord);
                          run.push_back(runRecord);
                     }
                     measuringPage->EmptyItOut();
                     appended = measuringPage->Append(&nextRecord);
                     
                     pagesRead++;
                     // If we already have read runLength of pages, then a run is complete.
                     // Sort it and and write it to the temporary file
                     if( pagesRead >= bigQ->runLength) {
                         runComplete = true;
                         break;
                     }
                 }
           } while( bigQ->inPipe->Remove(&nextRecord) );
           
           // There are two ways the above do-while loop ends
           // Way1: One run is complete
           // Way2: Input pipe exhausted, but run (i.e, last run) is incomplete
           // If it ended in way2, there'll be a half full tempPage. Don't miss
           // to transfer the contents of this half full tempPage to run
           if(runComplete == false) {
                 Record *tempRecord = new (std::nothrow) Record;
                 if(tempRecord == NULL) {
                      cout<<"Not enought memory.\n";
                      exit (1);
                 }
        
                 while( measuringPage->GetFirst(tempRecord) ) {
                      // Create space for a new run record
                      Record *runRecord = new (std::nothrow) Record;
                      runRecord->Consume(tempRecord);
                      run.push_back(runRecord);
                 }
                 measuringPage->EmptyItOut();
                 delete tempRecord;
           }
                  
           // Sort the run
           sort( run.begin(), run.end(), Comparator(bigQ->sortOrder) );
           
           // Append each run to the temporary file
           
           // We keep track of the page in the temp file where each run starts.
           // Because after sorting each run might take more or less than runLength pages.
           // So we do this and can't rely on that the start of each run will be a multiple of runLength.
       	   runStartPageIndexes.push_back( tempFilePageIndex );
       	   
       	   Page *measuringPage2 = new (std::nothrow) Page;
           if(measuringPage2 == NULL) {
                 cout<<"Not enought memory.\n";
                 exit (1);
           }

           for(vector<Record *>::const_iterator iter = run.begin();
                             iter != run.end(); iter++ ) 
           {   
                  // Add the record to the measuringPage2
                  int appended = measuringPage2->Append( (*iter) );
                  
                  // Check if measuringPage2 is full
                  if(! appended) {
                       // Write measuringPage2 to the tempFile
                       tempFile.AddPage(measuringPage2, tempFilePageIndex);
                       measuringPage2->EmptyItOut();
                       
                       tempFilePageIndex++;
                       
                       measuringPage2->Append( (*iter));
                  }
                  
                  delete (*iter);
                  //counter++;
           }
           
           // Write the last half full page
           tempFile.AddPage(measuringPage2, tempFilePageIndex);
           
           // Store the index of temp file page where this run ends
           runEndPageIndexes.push_back(tempFilePageIndex);
           tempFilePageIndex++;
           
           measuringPage2->EmptyItOut();
           delete measuringPage2;
           
           // Empty the run
           run.clear();
    } // Start another run
   	
   	// End of phase1. All runs are constructed and put in the temporary file
    delete measuringPage;
    
    // Phase 2
    // Construct a priority queue and keep pushing pages of records from each run into it.
    // Keep poping records (which'll be in sorted order) from the priority queue into the out pipe  
 	
 	// Before pushing records from runs into priroityQueue, each record is
 	// wrapped up as a priorityQueueRecord with run information attached to it.
    // This is to identify to which run that particular record belongs to
	priority_queue<PriorityQueueRecord*, vector<PriorityQueueRecord *>, Comparator> 
                                         priorityQ( Comparator(bigQ->sortOrder) );
	
    unsigned int numOfRuns = runStartPageIndexes.size();
    
    // No:of records of each run that are in the priorityQ
	int *numOfRecordsInPQ = new int[numOfRuns];
	int *done = new int[numOfRuns];
	
	Page *tempPage = new (std::nothrow) Page;
    if(tempPage == NULL) {
         cout<<"Not enought memory.\n";
         exit (1);
    }
           
	// Load the priorityQ with first page from every run
	for (unsigned int ithRun = 0; ithRun < numOfRuns; ithRun++) {
        
		tempFile.GetPage(tempPage, runStartPageIndexes[ithRun]);
		
		// Do we need this here?
		if (runEndPageIndexes[ithRun] == runStartPageIndexes[ithRun]) {
			done[ithRun] = 1;
		} else {
			done[ithRun] = 0;
		}
		
        // Point to the next unread page of the run
		runStartPageIndexes[ithRun]++;
		
		numOfRecordsInPQ[ithRun] = 0;
		while ( tempPage->GetFirst(&nextRecord) ) {
            Record* newRecord = new Record();
             if(newRecord == NULL) {
                 cout<<"Not enought memory.\n";
                 exit (1);
            }
			newRecord->Consume(&nextRecord);
                          
			priorityQ.push(new PriorityQueueRecord(newRecord, ithRun));
			numOfRecordsInPQ[ithRun]++;
		}
	}
	
	while (!priorityQ.empty()) {
        // Get the smallest record from the priorityQ
		PriorityQueueRecord *nextPQRecord = priorityQ.top();
		priorityQ.pop();
		
		// Put this smallest record in the output pipe
		bigQ->outPipe->Insert( nextPQRecord->GetRecord() );
		
		int whichRun = nextPQRecord->GetRunIndex(); // To whichRun the popped record belongs to
		
		numOfRecordsInPQ[whichRun]--;
		// Check if there are still records of the whichRun in the priorityQ
		if ( ! numOfRecordsInPQ[whichRun] ) {
            // Check if there are still unread pages in whichRun 
			if ( ! done[whichRun] ) {
                 // Load the next page from the run into the priorityQ
				tempFile.GetPage(tempPage, runStartPageIndexes[whichRun]);
				
				if (runEndPageIndexes[whichRun] == runStartPageIndexes[whichRun]) {
					done[whichRun] = 1;
				} else {
					done[whichRun] = 0;
				}
				
				// Point to the next unread page of the run
				runStartPageIndexes[whichRun]++;
				
				numOfRecordsInPQ[whichRun] = 0;
    			while ( tempPage->GetFirst(&nextRecord) ) {
                    Record* newRecord = new Record();
                    if(newRecord == NULL) {
                        cout<<"Not enought memory.\n";
                        exit (1);
                    }
    			    newRecord->Consume(&nextRecord);
            
        			priorityQ.push(new PriorityQueueRecord(newRecord, whichRun));
        			numOfRecordsInPQ[whichRun]++;
    		    }
			}
		}
		delete nextPQRecord;
	}
	
	// Delete the temporary file
	remove(tempFileName);
	
	delete tempPage;
	delete [] numOfRecordsInPQ;
	delete [] done;
	// End of Phase2
	
	// finally shut down the out pipe
	bigQ->outPipe->ShutDown();
}

BigQ::~BigQ () {
}

// PriorityQueueRecord class functions
PriorityQueueRecord :: PriorityQueueRecord(Record *record, unsigned int runIndex)
{
	this->record = record;
	this->runIndex = runIndex;
}

Record *PriorityQueueRecord :: GetRecord() {
	return record;
}

int PriorityQueueRecord :: GetRunIndex()
{
	return runIndex;
}
