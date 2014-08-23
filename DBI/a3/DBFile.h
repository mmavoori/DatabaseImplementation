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

typedef enum {
	heap, sorted, tree
	} fType;

struct SortInfo {
       OrderMaker *myOrder;
       int runLength;
};

void *collectFromBigQ(void *arg);
void *mergeToSortedDBFile(void *arg);

class GenericDBFile {
    friend class DBFile;
protected:
    File mFile;
	// write buffer
	Page *mWritePageBuffer;
	// read buffer
    Page *mReadPageBuffer;
	off_t mPageIndex;
	// file name
	char *mFileName;
    
    int GetFileLength();  

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


// sub class for Generic DB file for Heap File
class HeapDBFile : public GenericDBFile {
private:
    
	bool mIsWriteBufferDirty;
    bool mIsCopyOfLastPage;
	int mRecordIndex;
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


// sub class for Generic db file for sorted file implementation
class SortedDBFile : public GenericDBFile {
    friend void *collectFromBigQ(void *arg);
    friend void *mergeToSortedDBFile(void *arg);
private:
    SortInfo *mSortInfo;
    BigQ *mBigQ;
    Pipe *mToBigQPipe;
    Pipe *mFromBigQPipe;
    char mode;
	Page *mSearchPage;
	int mSearchPageIndex;
	OrderMaker *mSearchOrderMaker;
	OrderMaker *mLiteralOrderMaker;
	
    void AddToWriteBuffer (Record &addme);
    
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

// DBFile class
class DBFile {
private:
    GenericDBFile *myInternalVar;
public:
	DBFile();
	int Create(char *file_path, fType file_type, void *startup);
	int Open(char *file_path);
	void MoveFirst();
	int Close();
	void Load(Schema &myschema, char *load_file_path);
	void Add(Record &addme);
	int GetNext(Record &fetchme);	
	int GetNext(Record &fetchme, CNF &applyMe, Record &literal);
};

#endif
