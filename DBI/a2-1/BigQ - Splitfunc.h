#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include<vector>
#include<algorithm>
#include <queue>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

// workerThread's function
void *startWork(void *arg);

class BigQ {

friend void *startWork(void *arg);

private:
        pthread_t workerThread; // Thread that implements the tpmms
        Pipe *inPipe;           // Get unsorted records from here
        Pipe *outPipe;          // Put the sorted records in here
        OrderMaker *sortOrder;  // SortOrder specifying the sort key
        int runLength;          // Length of each run in pages
        Schema *schema; // DEBUG
        
        void tpmms_phase1();
        void tpmms_phase2();
        
        char tempFileName[25];           // Temporary file to write the sorted runs
        File tempFile;                   
        vector<int> runStartPageIndexes; // To keep track of where each run starts in the temp file
        vector<int> runEndPageIndexes;   // To keep track of where each run ends in the temp file
        
public:
       // Initialize the BigQ object and start the worker thread
	   BigQ (Pipe &inPipe, Pipe &outPipe, OrderMaker &sortOrder, int runLength, Schema* schema);
	   
       ~BigQ ();
};

// Class to represent records in the PriorityQueue by attaching run index to records
// in order to indentify to which run the records belongs to
class PriorityQueueRecord {
	
private:
    Record *record;        // Normal record
	unsigned int runIndex; // Index of run that this record belongs to

public:
	PriorityQueueRecord(Record *record, unsigned int runIndex);
	
	int GetRunIndex();
	Record *GetRecord();
};

#endif
