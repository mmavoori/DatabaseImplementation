#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

void* SelectPipeWorker(void* noargument);
void* SelectFileWorker(void* noargument);
void* ProjectWorker(void* iProjectObj);
void* JoinWorker(void* iJoinObj);
void* SumWorker(void* iSumObj);
void* DuplicateRemovalWorker(void* iDupObj);
void* WriteOutWorker(void* iWriteOutObj);
void* GroupByWorker(void* iGroupByObj);


class RelationalOp {
	public:
	// blocks the caller until the particular relational operator
	// has run to completion
	virtual void WaitUntilDone () = 0;

	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp {

	private:
	   pthread_t mFileThread;
	   Record* mFileLiteral;
	   DBFile* mInputFile;
	   CNF* mFileSelOp;
	   Pipe* mFileOutPipe;

	public:

	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone (){
	    pthread_join(mFileThread, NULL);
	}
	void Use_n_Pages (int n);
    friend void* SelectFileWorker(void* noargument);
};

class SelectPipe : public RelationalOp {
    private:
        Pipe* mInPipe;
        Pipe* mOutPipe;
        CNF* mSelOp;
        Record* mLiteral;
        pthread_t mPipeThread;

	public:
    	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	    void WaitUntilDone () {
	        // wait until the Run Method completes
	        pthread_join(mPipeThread, NULL);
	    }
    	void Use_n_Pages (int n) { }
    	// friend function in order to access the private variables
    	friend void* SelectPipeWorker(void* noargument);
};
class Project : public RelationalOp {
    private:
        Pipe* mInPipe;
        Pipe* mOutPipe;
        int* mKeepMe;
        pthread_t mProjectThread;
        int mNumAttsInput;
        int mNumAttsOutput;
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone () {
        	// wait until the Run Method completes
	        pthread_join(mProjectThread, NULL);
	    }
	void Use_n_Pages (int n) { }
	friend void* ProjectWorker(void* iProjectObj);
};
class Join : public RelationalOp {
    private:
        Pipe* mInPipeL;
        Pipe* mInPipeR;
        Pipe* mOutPipe;
        CNF* mSelOp;
        Record* mLiteral;
        pthread_t mJoinThread;
        int runLength;
	public:
    	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	    void WaitUntilDone () {
	        // wait until the Run Method completes
	        pthread_join(mJoinThread, NULL);
	    }
    	void Use_n_Pages (int n) {
    	   this->runLength = n;
    	}
    	friend void* JoinWorker(void* iJoinObj);
};
class DuplicateRemoval : public RelationalOp {
	private:
		Pipe* mInPipe;
    	Pipe* mOutPipe;
    	Schema* mSchema;
    	pthread_t mDupRemovalThread;
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone () {
		pthread_join(mDupRemovalThread, NULL);
	}
	void Use_n_Pages (int n) { }
	friend void* DuplicateRemovalWorker(void* iDupObj);
};
class Sum : public RelationalOp {
	private:
		Pipe* mInPipe;
    	Pipe* mOutPipe;
    	Function* mFunc;
    	pthread_t mSumThread;
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	void WaitUntilDone () {
		pthread_join(mSumThread, NULL);
	}
	void Use_n_Pages (int n) { }
	friend void* SumWorker(void* iSumObj);
};
class GroupBy : public RelationalOp {
	private:	
		Pipe* mInPipe;
    	Pipe* mOutPipe;
    	OrderMaker* mOrderMaker;
    	Function* mFunc;
    	pthread_t mGroupByThread;
    	int lRunLength;
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone () { 
		pthread_join(mGroupByThread, NULL);
	}
	void Use_n_Pages (int n) { 
		this->lRunLength = n;
	}
	friend void* GroupByWorker(void* iGroupByObj);
};
class WriteOut : public RelationalOp {
	private:
		Pipe* mInPipe;
		FILE* mOutFile;
		Schema* mMySchema;
		pthread_t mWriteOutThread;
	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone () {
		pthread_join(mWriteOutThread, NULL);
	 }
	void Use_n_Pages (int n) { }
	friend void* WriteOutWorker(void* iWriteOutObj);
};
#endif
