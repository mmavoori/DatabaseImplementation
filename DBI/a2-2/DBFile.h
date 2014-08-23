#ifndef DBFILE_H
#define DBFILE_H

#include<iostream>
#include<fstream>
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "BigQ.h"
#include "Pipe.h"
using namespace std;

typedef enum {heap, sorted, tree} fType;

struct SortInfo {
       OrderMaker *myOrder;
       int runLength;
};

void *collectFromBigQ(void *arg);
void *mergeToSortedDBFile(void *arg);

// DBFile is used for storing and retrieving records from the disk.
// Two pages in main memory(one for writing and one for reading) are used as buffers.
// Data transfer between memory and disk is done in pages in order to reduce the
// disk I/O costs.
//
// Reading:
//         First the read buffer is checked. If it's not empty, then a record is 
//         fetched from it. But if the read buffer is empty, then the next page 
//         from the file(on disk) is read into the read buffer(in memory) and
//         the record is fetched.
// Writing:
//         The record is added to the write buffer, where it waits to be written 
//         to the file. But if the write buffer is full, then it is written to
//         the file(on disk) and the record is added to a fresh empty write buffer.


// Virtual base class for various flavours of DBFile
class GenericDBFile {
    friend class DBFile;
protected:
    File file;               // Binary file handle
	char *fileName;          // Name of the binary file
    Page *writePageBuffer;   // write buffer in memory
    Page *readPageBuffer;    // read buffer in memory
    
    off_t curReadPageIndex;  // which page from the file are we currently reading

    // Functions
    int GetFileLength();     // Function that returns correct filelength in pages

public:
	GenericDBFile(); 
	
	virtual int Open (char *file_path) =0;
	virtual void MoveFirst()=0;
	virtual int Close() =0;
	virtual void Load(Schema &myschema, char *load_file_path) =0;
	virtual void Add(Record &addme) =0;
	virtual int GetNext(Record &fetchme) =0;
	virtual int GetNext(Record &fetchme, CNF &applyMe, Record &literal) =0;
};


// Class for Heap type of DBFile
class HeapDBFile : public GenericDBFile {
private:
    int recordIndex;         // Index of the record, in the read buffer, that we are currently reading
    
    bool writeBufferDirty;   // flags associated with write buffer
    bool writeBufferCopyOfLastPage;
    
public:
	HeapDBFile(); 

    int Open (char *file_path);
    void MoveFirst();
	int Close();
	void Load(Schema &myschema, char *load_file_path);
	void Add(Record &addme);
	int GetNext(Record &fetchme);
	int GetNext(Record &fetchme, CNF &applyMe, Record &literal);
};


// Class for Sorted type of DBFile
// Note: The functionality of Add() and GetNext() are different for SortedDBFile
// and HeapDBFile.
// SortedDBFile uses 'writePageBuffer' only during Load()
class SortedDBFile : public GenericDBFile {
    friend void *collectFromBigQ(void *arg);
    friend void *mergeToSortedDBFile(void *arg);
private:
    SortInfo *sortInfo;
    BigQ *bigQ;
    Pipe *toBigQPipe;       // input pipe to the bigQ
    Pipe *fromBigQPipe;     // output pipe from the bigQ
    char mode;              // Whether SortedDBFile is in "reading" or "writing" or "loading" or "merging" mode
                            // Only GetNext() and Add() functions use and change mode.
    
	Page *searchPage;       // Read buffer used in binary search
	int searchPageIndex;    // Index of the search page
	OrderMaker *searchOrderMaker;    	// Order maker used for search
	OrderMaker *literalOrderMaker;     	// Order maker for literal
	
	// Functions
    void AddToWriteBuffer (Record &addme); // Add a record to writePageBuffer
    
public:
	SortedDBFile (void *startup, ofstream& fout);
    SortedDBFile (ifstream &fin); 

    int Open (char *file_path);
    void MoveFirst();
	int Close();
	void Load(Schema &myschema, char *load_file_path);
	void Add(Record &addme);
	int GetNext(Record &fetchme);
	int GetNext(Record &fetchme, CNF &applyMe, Record &literal);
};

// DBFile class that encapsulates the various flavours and  is visible to the outside world
class DBFile {
private:
    GenericDBFile *myInternalVar;
public:
	DBFile();

    // create a new file
	int Create(char *file_path, fType file_type, void *startup);
	
	// open a file that already exists
	int Open(char *file_path);
	
	// Move to the first record in the file
	void MoveFirst();
	
	// close the file
	int Close();

    // Load relation data from a text file and create the binary file equivalent
	void Load(Schema &myschema, char *load_file_path);
	
	// Add a record to the file
	void Add(Record &addme);
	
    // Fetch the next record from the file in sequential order
	int GetNext(Record &fetchme);
	
	// Fetch the next record from the file that satisfies the CNF
	int GetNext(Record &fetchme, CNF &applyMe, Record &literal);
};

#endif
