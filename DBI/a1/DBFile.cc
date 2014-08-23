#include <iostream>
#include <cstdlib>
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

using namespace std;

// stub file .. replace it with your own DBFile.cc

DBFile::DBFile (): mPageIndex(0), mIsWriteBufferDirty(false),
                        mIsWriteAtLastpage(false){
    mFile = new File();
    mWriteBuffer = new Page();
    mReadBuffer = new Page();
}

// Destructor
DBFile::~DBFile () {
    delete mReadBuffer;
    delete mWriteBuffer;
    delete mFile;
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
    int lResult = 1;
    switch(f_type){
    case heap:
        {
            cout<<"creating heap file.."<<endl;
            mFile->Open(0, f_path);
            cout<<"created heap file: "<<f_path<<endl;
            break;
        }
    case sorted:
        {
            cout<<"File is of type Sorted"<<endl;
            break;
        }
    case tree:
        {
            cout<<"File is of type Tree"<<endl;
            break;
        }
    default:
        cout<<"File type not valid!"<<endl;
        lResult = 0;
    }
    //mFile->Close();
    return lResult;
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
    FILE* loadFile = fopen(loadpath, "r");
    if(loadFile == NULL){
        cerr<<"Error in opening the file: "<<loadpath<<endl;
        exit(1);
    }
    Record lTempRecord;
    int count = 0;
    while(lTempRecord.SuckNextRecord(&f_schema, loadFile)){
        lTempRecord.Print(&f_schema);
        Add(lTempRecord);
        ++count;
    }
    cout<<"Loaded "<<count<<" records from text file"<<endl;
    //cout<<"Length of file is : "<<mFile->GetLength()<<endl;
    fclose(loadFile);
}

int DBFile::Open (char *f_path) {
    if(f_path == NULL) {
        return 0;
    }
    cout<<"Opening file : "<<f_path<<endl;
    mFile->Open(1, f_path);

    mIsWriteAtLastpage = true;
    return 1;
}

void DBFile::MoveFirst () {
    // Empty read buffer
    mReadBuffer->EmptyItOut();
    // Get the first page and put it in mWriteBuffer
    mFile->GetPage (mReadBuffer, 0);
    // Get the first record and put it in mCurrentRecord
    mPageIndex = 0;
}

int DBFile::Close () {
    // if write buffer is dirty, write it to new page

    if(mIsWriteBufferDirty){
        cout<<"In DBFile close: write buffer is dirty"<<endl;
        if(mIsWriteAtLastpage){
            cout<<"write is last page, overwriting.."<<endl;
            mFile->AddPage(mWriteBuffer, mFile->GetLength()-1);
        } else {
            cout<<"write is not at last page, writing to new page.."<<endl;
            mFile->AddPage(mWriteBuffer, mFile->GetLength());
        }
        mWriteBuffer->EmptyItOut();
        mIsWriteBufferDirty = 0;
        mIsWriteAtLastpage = false;
    }

    // empty out write buffer
    mWriteBuffer->EmptyItOut();
    mFile->Close();
    return 1;
}

void DBFile::Add (Record &rec) {

    cout<<"\nInside DBFile Add method"<<endl;
    Record lTemp;
    lTemp.Copy(&rec);
	if(mWriteBuffer->Append(&rec)==0) {
	    mFile->AddPage(mWriteBuffer, mFile->GetLength()-1);
		mWriteBuffer->EmptyItOut();
		mWriteBuffer->Append(&lTemp);
	}
    mIsWriteBufferDirty = 1;
}

int DBFile::GetNext (Record &fetchme) {
    // if no records present in read buffer

    if(mReadBuffer->GetFirst(&fetchme) == 0){
        //++mPageIndex;
        if(++mPageIndex >= mFile->GetLength()-1){
            return 0;
        }
        // read next page

        mReadBuffer->EmptyItOut();
        mFile->GetPage(mReadBuffer, mPageIndex);
        // get the next record from the read buffer
        mReadBuffer->GetFirst(&fetchme);


    }
    return 1;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    // create a comparison engine
    ComparisonEngine comparisionEngine;
	do {
    	int lTemp = GetNext(fetchme);
		if (lTemp == 0) {
		// finished reading the entire file
			return 0;
		}
	} while (!comparisionEngine.Compare(&fetchme, &literal, &cnf));

	return 1;
}
