#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include<vector>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

void *tpmms(void *arg);     // workerThread's function

class BigQ {

friend void *tpmms(void *arg);

private:
        pthread_t workerThread; // Thread that implements the tpmms
        Pipe *inPipe;           // Get unsorted records from here
        Pipe *outPipe;          // Put the sorted records in here
        OrderMaker *sortOrder;  // SortOrder specifying the sort key
        int runLength;          // Length of each run in pages
        
public:
       // Initialize the BigQ object and start the worker thread
	   BigQ (Pipe &inPipe, Pipe &outPipe, OrderMaker &sortOrder, int runLength);
	   
       ~BigQ ();
};

// Class to represent records in the PriorityQueue. Run index is attached to records
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
