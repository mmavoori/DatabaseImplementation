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

int GenericDBFile::GetFileLength()
{
    if(mFile.GetLength() == 0)
        return mFile.GetLength();
    else
        return mFile.GetLength() - 1;
}

GenericDBFile::GenericDBFile()
{
    mReadPageBuffer = new (std::nothrow) Page;
    if(mReadPageBuffer == NULL)
    {
        cout << "No enough memory\n";
		exit(1);
    }
    mWritePageBuffer = new (std::nothrow) Page;
    if(mWritePageBuffer == NULL)
    {
        cout << "No Not enough memory\n";
		exit(1);
    }
}

HeapDBFile::HeapDBFile() : GenericDBFile()
{
    mIsWriteBufferDirty = 0;
    mIsCopyOfLastPage = 0;
    mPageIndex = -2;
}

int HeapDBFile::Open (char *filePath)
{
    mFile.Open(1,filePath);

       if(GetFileLength() > 0) {
         mPageIndex = 0;
         mFile.GetPage(mReadPageBuffer, mPageIndex);
         mRecordIndex = 0;
    }

    if(GetFileLength() > 0) {
         mFile.GetPage(mWritePageBuffer, GetFileLength() - 1);
         mIsWriteBufferDirty = 0;
         mIsCopyOfLastPage = 1;
    }

    return 1;
}

void HeapDBFile::MoveFirst()
{
     mReadPageBuffer->EmptyItOut();
     mFile.GetPage(mReadPageBuffer,0);
     mRecordIndex = 0;
     mPageIndex = 0;
}

int HeapDBFile::Close()
{
    if(mIsWriteBufferDirty) {
         if(mIsCopyOfLastPage == 1) {
              mFile.AddPage(mWritePageBuffer, GetFileLength() - 1);
          }
          else {
              mFile.AddPage(mWritePageBuffer, GetFileLength());
          }

          mWritePageBuffer->EmptyItOut();
          mIsWriteBufferDirty = 0;
          mIsCopyOfLastPage = 0;
    }

    int lFileLength = mFile.Close();
    delete mReadPageBuffer;
    delete mWritePageBuffer;
    return lFileLength;
}


void HeapDBFile::Load (Schema &file_schema, char *file_path)
{
     FILE *file = fopen(file_path,"r");
     if(file == NULL)
     {
             cerr<< "failed opening the file";
             exit(1);
     }

     Record lRecordTemp;

     int counter = 0;
	 while (lRecordTemp.SuckNextRecord(&file_schema, file)) {
		Add(lRecordTemp);
        counter++;
	 }
     cout<<"Loaded " << counter << " records from the text file.\n";

	 fclose(file);
}

void HeapDBFile::Add (Record &addme)
{
      int lAppended = mWritePageBuffer->Append(&addme);

     if(!lAppended) {
          if(mIsCopyOfLastPage == 1) {
              mFile.AddPage(mWritePageBuffer, GetFileLength() - 1);
          }
          else {
              mFile.AddPage(mWritePageBuffer, GetFileLength());
          }
          mWritePageBuffer->EmptyItOut();
          mIsWriteBufferDirty = 0;
          mIsCopyOfLastPage = 0;
          lAppended = mWritePageBuffer->Append(&addme);
     }

     mIsWriteBufferDirty = 1;
}

int HeapDBFile::GetNext (Record &fetchme)
{
    if((mPageIndex == GetFileLength() - 1) && mIsWriteBufferDirty == 1) {

          if(mIsCopyOfLastPage == 1) {
              mFile.AddPage(mWritePageBuffer, GetFileLength() - 1);
              mIsCopyOfLastPage = 1;
              mIsWriteBufferDirty = 0;
              mReadPageBuffer->EmptyItOut();
              mFile.GetPage(mReadPageBuffer, GetFileLength() - 1);
              mPageIndex == GetFileLength() - 1;
              Record lRecordTemp;
              for (int i = 0; i < mRecordIndex; i++) {
    			mReadPageBuffer->GetFirst(&lRecordTemp);
    		  }
        }
    }
    int lNext = mReadPageBuffer->GetFirst(&fetchme);
    if(!lNext) {
          if(mPageIndex == (GetFileLength() - 1)) {
               if(mIsWriteBufferDirty) {
                      mFile.AddPage(mWritePageBuffer, GetFileLength());
                      mIsCopyOfLastPage = 1;
                      mIsWriteBufferDirty = 0;
                }
                else {
                     return 0; // finished reading the entire file
                }
          }
          mReadPageBuffer->EmptyItOut();
          mFile.GetPage(mReadPageBuffer, ++mPageIndex);
          mRecordIndex = 0;
          lNext = mReadPageBuffer->GetFirst(&fetchme);
    }
    mRecordIndex++;
    return 1;
}

int HeapDBFile::GetNext (Record &fetchme, CNF &applyMe, Record &literal)
{
    ComparisonEngine comparisionEngine;
	do {
		if (GetNext(fetchme) == 0) {
			return 0;
		}
	} while (!comparisionEngine.Compare(&fetchme, &literal, &applyMe));

	return 1;
}

//Code for SortedDBFile
SortedDBFile::SortedDBFile (void *startup, ofstream& fout) : GenericDBFile()
{
    mSortInfo = (SortInfo *) startup;
    mSortInfo->myOrder->WriteToFile(fout);
    fout << mSortInfo->runLength << endl;

    mPageIndex = -2;
    mSearchPage = NULL;
	mSearchOrderMaker = NULL;
	mLiteralOrderMaker = NULL;
    mode = 'r';
}

SortedDBFile::SortedDBFile (ifstream &fin) : GenericDBFile()
{

    mSortInfo = new SortInfo();
    mSortInfo->myOrder = new OrderMaker(fin);
    fin >> mSortInfo->runLength;
    mPageIndex = -2;
    mSearchPage = NULL;
	mSearchOrderMaker = NULL;
	mLiteralOrderMaker = NULL;
    mode = 'r';
}

int SortedDBFile::Open (char *file_path)
{
    mFile.Open(1,file_path);
    if(GetFileLength() > 0) {   // means not an empty file
         mPageIndex = 0;
         mFile.GetPage(mReadPageBuffer, mPageIndex);
    }
    mode = 'r';
    return 1;
}

void SortedDBFile::MoveFirst()
{
     mReadPageBuffer->EmptyItOut();
     mFile.GetPage(mReadPageBuffer,0);
     mPageIndex = 0;
}

int SortedDBFile::Close()
{
    if(mode == 'l') {
        mFile.AddPage(mWritePageBuffer, GetFileLength());
        mWritePageBuffer->EmptyItOut();
    }


    if(mode == 'w') {
         mToBigQPipe->ShutDown();
    	 pthread_t thread3;
    	 if(GetFileLength() == 0) {
              // empty SortedDBFile
              pthread_create (&thread3, NULL, collectFromBigQ, (void *)this);
         }
         else {
              // merging SortedDBFile
              pthread_create (&thread3, NULL, mergeToSortedDBFile, (void *)this);
         }

    	 pthread_join (thread3, NULL); // wait till the thread completes

    	 // Deleting instances
    	 delete mToBigQPipe;
    	 delete mFromBigQPipe;
    	 delete mBigQ;
    }
    int lFileLength = mFile.Close();
    delete mReadPageBuffer;
    delete mWritePageBuffer;

    return lFileLength;
}

void SortedDBFile::Load (Schema &file_schema, char *file_path)
{

     mode = 'l';

     FILE *file = fopen(file_path,"r");
     if(file == NULL)
     {
             cerr<< "Opening file \""<< file_path <<"\" failed.\n";
             exit(1);
     }

     // TO Create pipes
     mToBigQPipe = new Pipe(100);
     mFromBigQPipe = new Pipe(100);

     // mBigQ instance to automatically start a thread here.
     mBigQ = new BigQ(*mToBigQPipe, *mFromBigQPipe, *(mSortInfo->myOrder), mSortInfo->runLength);

     // Getting each record from text file, adding to 'mToBigQPipe'
     Record lRecordTemp;
	 while (lRecordTemp.SuckNextRecord(&file_schema, file)) {
		mToBigQPipe->Insert(&lRecordTemp);
	 }
	 mToBigQPipe->ShutDown();
	 pthread_t thread2;
	 pthread_create (&thread2, NULL, collectFromBigQ, (void *)this);
	 pthread_join (thread2, NULL); // wait till the thread completes
	 //Deleting pipes created
	 delete mToBigQPipe;
	 delete mFromBigQPipe;
	 delete mBigQ;
	 fclose(file);
}

void *collectFromBigQ(void *arg)
{
     SortedDBFile *lSortedDBFile = (SortedDBFile *) arg;

     Record lRecordTemp;
     int counter = 0;
	 while ( lSortedDBFile->mFromBigQPipe->Remove(&lRecordTemp) ) {
        lSortedDBFile->AddToWriteBuffer(lRecordTemp);
		counter++;
	 }
	 lSortedDBFile->mFile.AddPage(lSortedDBFile->mWritePageBuffer, lSortedDBFile->GetFileLength());
     lSortedDBFile->mWritePageBuffer->EmptyItOut();
	 lSortedDBFile->mFromBigQPipe->ShutDown();
}

void SortedDBFile::AddToWriteBuffer (Record &addme)
{
     int lAppend = mWritePageBuffer->Append(&addme);
     if(!lAppend) {
          mFile.AddPage(mWritePageBuffer, GetFileLength());
          mWritePageBuffer->EmptyItOut();
          lAppend = mWritePageBuffer->Append(&addme);
     }
}

void SortedDBFile::Add(Record &addme)
{
     if(mode == 'r') {
         mToBigQPipe = new Pipe(100);
         mFromBigQPipe = new Pipe(100);
         mBigQ = new BigQ(*mToBigQPipe, *mFromBigQPipe, *(mSortInfo->myOrder), mSortInfo->runLength);
         mode = 'w';
     }

     mToBigQPipe->Insert(&addme);
}

int SortedDBFile::GetNext (Record &fetchme)
{
     if(mode == 'w') {
         mToBigQPipe->ShutDown();
    	 pthread_t thread3;
    	 pthread_create (&thread3, NULL, mergeToSortedDBFile, (void *)this);
    	 pthread_join (thread3, NULL); // wait till the thread completes
    	 delete mToBigQPipe;
    	 delete mFromBigQPipe;
    	 delete mBigQ;

         mode = 'r'; // Change to reading mode
     }

    int lNext = mReadPageBuffer->GetFirst(&fetchme);
    if(!lNext) {
          if(mPageIndex == (GetFileLength() - 1)) {
               return 0; // finished reading the entire file
          }
          mReadPageBuffer->EmptyItOut();
          mFile.GetPage(mReadPageBuffer, ++mPageIndex);
          lNext = mReadPageBuffer->GetFirst(&fetchme);
    }

    return 1;
}

void *mergeToSortedDBFile(void *arg)
{
    SortedDBFile *lSortedDBFile = (SortedDBFile *) arg;
    lSortedDBFile->mode = 'm';
    ComparisonEngine lComparisonEngine;
	char lMergeFileName[100];
	sprintf(lMergeFileName, "%s_MergeFile%d", lSortedDBFile->mFileName, getppid());
	File lMergeFile;
	lMergeFile.Open(0, lMergeFileName);
    Page lMergeReadPageBuffer;
	int lReadPageIndex = 0;
    Page lMergeWritePageBuffer;
    int lMergeFilePageIndex = 0;
 	bool lFileHasMore = 0;
	Record lSortedDBFileRecord;
	if (lSortedDBFile->GetFileLength() > 0) {
		lSortedDBFile->mFile.GetPage(&lMergeReadPageBuffer, lReadPageIndex);
		lFileHasMore = lMergeReadPageBuffer.GetFirst(&lSortedDBFileRecord);
	}
	Record lAddedRecord;
	bool pipeHasMore = lSortedDBFile->mFromBigQPipe->Remove(&lAddedRecord);
	while (lFileHasMore || pipeHasMore) {

        if (lFileHasMore && !pipeHasMore) {
           while(lFileHasMore) {
    			int lAppend = lMergeWritePageBuffer.Append(&lSortedDBFileRecord);
    			if (!lAppend) {
    				lMergeFile.AddPage(&lMergeWritePageBuffer, lMergeFilePageIndex++);
    				lMergeWritePageBuffer.EmptyItOut();
    				lMergeWritePageBuffer.Append(&lSortedDBFileRecord);
    			}

    			int lNext = lMergeReadPageBuffer.GetFirst(&lSortedDBFileRecord);
    			if (!lNext) {
    				lReadPageIndex++;

    				if ( lReadPageIndex == lSortedDBFile->GetFileLength() ) {
    				     lFileHasMore = 0;
    				}
                    else {
                         lSortedDBFile->mFile.GetPage(&lMergeReadPageBuffer, lReadPageIndex);
    					 lMergeReadPageBuffer.GetFirst(&lSortedDBFileRecord);
    				}
    			}
            }
            break;
        }


        if (pipeHasMore && !lFileHasMore) {
           while(pipeHasMore) {
    			int lAppend = lMergeWritePageBuffer.Append(&lAddedRecord);
    			if (!lAppend) {
    				lMergeFile.AddPage(&lMergeWritePageBuffer, lMergeFilePageIndex++);
    				lMergeWritePageBuffer.EmptyItOut();
    				lMergeWritePageBuffer.Append(&lAddedRecord);
    			}
    			pipeHasMore = lSortedDBFile->mFromBigQPipe->Remove(&lAddedRecord);
          }
          break;
        }

  	if ( lComparisonEngine.Compare(&lSortedDBFileRecord, &lAddedRecord,
                                   lSortedDBFile->mSortInfo->myOrder) <= 0) {
			int lAppend = lMergeWritePageBuffer.Append(&lSortedDBFileRecord);

			if (!lAppend) {
				lMergeFile.AddPage(&lMergeWritePageBuffer, lMergeFilePageIndex++);
				lMergeWritePageBuffer.EmptyItOut();
				lMergeWritePageBuffer.Append(&lSortedDBFileRecord);
			}
			int lNext = lMergeReadPageBuffer.GetFirst(&lSortedDBFileRecord);
			if (!lNext) {
				lReadPageIndex++;

				if ( lReadPageIndex == lSortedDBFile->GetFileLength() ) {
				     lFileHasMore = 0; // Finished reading all the records from the current SortedDBFile.
				}
                else {
                     lSortedDBFile->mFile.GetPage(&lMergeReadPageBuffer, lReadPageIndex);
					 lMergeReadPageBuffer.GetFirst(&lSortedDBFileRecord);
				}
			}
		}
        else {

			int lAppend = lMergeWritePageBuffer.Append(&lAddedRecord);
			if (!lAppend) {
				lMergeFile.AddPage(&lMergeWritePageBuffer, lMergeFilePageIndex++);
				lMergeWritePageBuffer.EmptyItOut();
				lMergeWritePageBuffer.Append(&lAddedRecord);
			}
			pipeHasMore = lSortedDBFile->mFromBigQPipe->Remove(&lAddedRecord);
		}
	}
	lMergeFile.AddPage(&lMergeWritePageBuffer, lMergeFilePageIndex++);
	lSortedDBFile->mFromBigQPipe->ShutDown();
	lMergeFile.Close();
	lSortedDBFile->mFile.Close();
	remove(lSortedDBFile->mFileName);
	rename(lMergeFileName, lSortedDBFile->mFileName);
	lSortedDBFile->mFile.Open(1, lSortedDBFile->mFileName);
}

int SortedDBFile::GetNext(Record &fetchMe, CNF &applyMe, Record &literal)
{
     if(mode == 'w') {
         mToBigQPipe->ShutDown();
    	 pthread_t thread4;
    	 pthread_create (&thread4, NULL, mergeToSortedDBFile, (void *)this);
    	 pthread_join (thread4, NULL);
    	 delete mToBigQPipe;
    	 delete mFromBigQPipe;
    	 delete mBigQ;
         mode = 'r';
     }

	ComparisonEngine lComparisonEngine;
  do {
    if (GetNext(fetchMe) == 0) {
      return 0;  // finished reading the entire file
    }
  } while (!lComparisonEngine.Compare(&fetchMe, &literal, &applyMe));

  return 1;
  /*
	if (mSearchPage == NULL) {
		OrderMaker *mySortOrder = mSortInfo->myOrder;
		if (mSearchOrderMaker != NULL) {
			delete mSearchOrderMaker;
		}
		mSearchOrderMaker = new OrderMaker();

		if (mLiteralOrderMaker != NULL) {
			delete mLiteralOrderMaker;
		}
		mLiteralOrderMaker = new OrderMaker();
		for (int i = 0; i < mySortOrder->GetNumOfAtts(); i++) {
			int att = mySortOrder->GetAttribute(i);
            int literalIndex = applyMe.HasSimpleEqualityCheck(att);
            if (literalIndex != -1) {
				mSearchOrderMaker->AddAttribute(att, mySortOrder->GetType(i));
				mLiteralOrderMaker->AddAttribute(literalIndex, mySortOrder->GetType(i));
			}
            else {
				break;
			}
		}
		off_t begin = 0;
		off_t end = mFile.GetLength() - 2;
		off_t mid = (begin + end) / 2;

        mSearchPage = new Page();

        int lCompareResult;
		int lPageWithEqualRecordFirst = -1;

        while (begin <= end) {
			mid = (begin + end) / 2;
			mFile.GetPage(mSearchPage, mid);
			mSearchPageIndex = mid;
			mSearchPage->GetFirst(&fetchMe);
			lCompareResult = lComparisonEngine.Compare(&fetchMe, mSearchOrderMaker, &literal, mLiteralOrderMaker);
			if (lCompareResult < 0) {
				int lGotNextRecord;
				do {
					lGotNextRecord = mSearchPage->GetFirst(&fetchMe);
					lCompareResult = lComparisonEngine.Compare(&fetchMe, mSearchOrderMaker, &literal, mLiteralOrderMaker);
				} while (lGotNextRecord && (lCompareResult < 0) );
				if (lCompareResult > 0 && lGotNextRecord) {
					return 0;
				}
				if (lCompareResult == 0) {
					break;
				}
				begin = mid + 1;
			}
            else {
 				if (lCompareResult == 0) {
					lPageWithEqualRecordFirst = mid;
				}
				lCompareResult = 1;
				end = mid - 1;
			}
		}
		if (lCompareResult == 0) {
		}
        else if (lPageWithEqualRecordFirst > -1) {
			mFile.GetPage(mSearchPage, mid);
			mSearchPageIndex = mid;
			mSearchPage->GetFirst(&fetchMe);
		}
        else {
			delete mSearchPage;
			mSearchPage = NULL;
			return 0;
		}
	}
    else {

		int lGotNextRecord = mSearchPage->GetFirst(&fetchMe);

        if (!lGotNextRecord){
            mSearchPageIndex++;
			if ( mSearchPageIndex < mFile.GetLength() - 1 ) {
				mFile.GetPage(mSearchPage, mSearchPageIndex);
				mSearchPage->GetFirst(&fetchMe);
			} else {
				return 0;
			}
		}
	}
	while (!lComparisonEngine.Compare(&fetchMe, &literal, &applyMe)) {
		if (lComparisonEngine.Compare(&fetchMe, mSearchOrderMaker, &literal, mLiteralOrderMaker) != 0) {
			return 0;
		}
		int lGotNextRecord = mSearchPage->GetFirst(&fetchMe);

        if (!lGotNextRecord){
            mSearchPageIndex++;
            if ( mSearchPageIndex < mFile.GetLength() - 1 ) {
				mFile.GetPage(mSearchPage, mSearchPageIndex);
				mSearchPage->GetFirst(&fetchMe);
			}
            else {
				return 0;
			}
		}
	}

	return 1;*/
}
DBFile::DBFile()
{ }

int DBFile::Create (char *file_path, fType file_type, void *startup)
{
    char lMetaFileName[50];
    sprintf(lMetaFileName, "%s.metadata", file_path);

    ofstream fout;
    fout.open(lMetaFileName);
    if(fout.fail()) {
        cout<<"\nOpening " << lMetaFileName << " file for writing failed.\n";
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

    myInternalVar->mFileName = new char[100];
	strcpy(myInternalVar->mFileName, file_path);

    myInternalVar->mFile.Open(0,file_path);

    fout.close();
    return 1;
}

int DBFile::Open (char *file_path)
{
    char lMetaFileName[50];
    sprintf(lMetaFileName, "%s.metadata", file_path);

    ifstream fin;
    fin.open(lMetaFileName);
    if(fin.fail()) {
        cout<<"failed opening the file\n";
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
        cout<<"invalid file type\n";
        exit(1);
    }

    myInternalVar->mFileName = new char[100];
	strcpy(myInternalVar->mFileName, file_path);

    return myInternalVar->Open(file_path);
}

void DBFile::MoveFirst()
{
    myInternalVar->MoveFirst();
}

int DBFile::Close()
{
	int lFileLength = myInternalVar->Close();
	delete myInternalVar;

	return lFileLength;
}

void DBFile::Load(Schema &file_schema, char *file_path)
{
    myInternalVar->Load(file_schema, file_path);
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
