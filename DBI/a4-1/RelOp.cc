#include "RelOp.h"
#include "ComparisonEngine.h"
#include <stdlib.h>

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {

	   mFileLiteral = &literal;
	   mInputFile = &inFile;
	   mFileSelOp = &selOp;
	   mFileOutPipe = &outPipe;

	   pthread_create(&mFileThread,NULL,SelectFileWorker,(void*)this);
}

void* SelectFileWorker(void* iSelectFileObj){
    Record lTempRecord;
    SelectFile* lConversionObj = (SelectFile*)iSelectFileObj;
    while(lConversionObj->mInputFile->GetNext(lTempRecord,
                *lConversionObj->mFileSelOp,*lConversionObj->mFileLiteral)!=0){
        //lConversionObj->mInputFile->GetNext(lTempRecord);
        //if(lTemp.Compare(&lTempRecord, lConversionObj->mFileLiteral,
          //                              lConversionObj->mFileSelOp)) {
              lConversionObj->mFileOutPipe->Insert(&lTempRecord);
            //}
    }
    lConversionObj->mFileOutPipe->ShutDown();

}


void SelectFile::Use_n_Pages (int runlen) {

}

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal){
    mInPipe = &inPipe;
    mOutPipe = &outPipe;
    mSelOp = &selOp;
    mLiteral = &literal;
    pthread_create(&mPipeThread, NULL, SelectPipeWorker, (void*)this);
}

void* SelectPipeWorker(void* iSelectPipeObj){
    ComparisonEngine lTemp;
    Record lTempRecord;
    SelectPipe* lConversionObj = (SelectPipe*)iSelectPipeObj;
    while(lConversionObj->mInPipe->Remove(&lTempRecord)){
         if(lTemp.Compare(&lTempRecord, lConversionObj->mLiteral, lConversionObj->mSelOp)) {
              lConversionObj->mOutPipe->Insert(&lTempRecord);
          }
    }
    lConversionObj->mOutPipe->ShutDown();
}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput){
    mInPipe = &inPipe;
    mOutPipe = &outPipe;
    mKeepMe = keepMe;
    mNumAttsInput = numAttsInput;
    mNumAttsOutput = numAttsOutput;
    pthread_create(&mProjectThread, NULL, ProjectWorker, (void*)this);
}

void* ProjectWorker(void* iProjectObj){
    Project *lConversionObj = (Project *) iProjectObj;
	Record lTempRecord;
    while(lConversionObj->mInPipe->Remove(&lTempRecord)) {
      //cout<<"in Project"<<endl;
         lTempRecord.Project(lConversionObj->mKeepMe,
                lConversionObj->mNumAttsOutput, lConversionObj->mNumAttsInput);
         lConversionObj->mOutPipe->Insert(&lTempRecord);
    }
    // Shutdown outpipe
    lConversionObj->mOutPipe->ShutDown();
}

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal){
    mInPipeL = &inPipeL;
    mInPipeR = &inPipeR;
    mOutPipe = &outPipe;
    mSelOp = &selOp;
    mLiteral = &literal;
    pthread_create(&mJoinThread, NULL, JoinWorker, (void*)this);
}

void* JoinWorker(void* iJoinObj){
    Join *lJoinObj = (Join *) iJoinObj;

    // output pipes to push the records from left and right input pipes
    Pipe* lOutPipeL = new Pipe(100);
    Pipe* lOutPipeR = new Pipe(100);

    // ordermakers for the two Big Qs'
    OrderMaker lLeftOrder;
    OrderMaker lRightOrder;
    if(lJoinObj->mSelOp->GetSortOrders(lLeftOrder, lRightOrder) != 0){ // only possible
    // to accept an ordering
     if(lJoinObj->runLength == 0) lJoinObj->runLength = 3;
        // took a value of 3 as a default. can be customized with a class variable
        BigQ* lLeftQ = new BigQ(*(lJoinObj->mInPipeL), *lOutPipeL, lLeftOrder, lJoinObj->runLength);
        BigQ* lRightQ = new BigQ(*(lJoinObj->mInPipeR), *lOutPipeR, lRightOrder, lJoinObj->runLength);

        ComparisonEngine lCompare;

        Record lLeftRecord;
        Record lRightRecord;

        int lIsLeftEmpty = lOutPipeL->Remove(&lLeftRecord);
        int lIsRightEmpty = lOutPipeR->Remove(&lRightRecord);

        while(lIsLeftEmpty && lIsRightEmpty){
            int lResult = lCompare.Compare(&lLeftRecord, &lLeftOrder, &lRightRecord, &lRightOrder);

            if(lResult < 0) { // right larger
                lIsLeftEmpty = lOutPipeL->Remove(&lLeftRecord);
            } else if (lResult > 0){ // left larger
                // remove the right record
                lIsRightEmpty = lOutPipeR->Remove(&lRightRecord);

            } else { // both are equal
                Record lMergeRecord;
                int numAttsLeft = ((((int*)(lLeftRecord.bits))[1])/sizeof(int)) - 1;
                int numAttsRight = ((((int*)(lRightRecord.bits))[1])/sizeof(int)) - 1;
                int numAttsToKeep = numAttsLeft + numAttsRight;
                int attsToKeep[numAttsToKeep];
                int i;
                for(i = 0; i < numAttsLeft; i++) {
                    attsToKeep[i] = i;
                }
                int startOfRight = i;
                for(int j = 0; j < numAttsRight; j++,i++) {
                    attsToKeep[i] = j;
                }
                lMergeRecord.MergeRecords(&lLeftRecord, &lRightRecord, numAttsLeft,
                        numAttsRight, attsToKeep, numAttsToKeep, startOfRight);
                lJoinObj->mOutPipe->Insert(&lMergeRecord);
            

           vector<Record *> tempRecArray;
           int tempRecIndex = 0;
           // store the left record in the temp records array
           tempRecArray.push_back( new Record );
           tempRecArray[tempRecIndex]->Copy(&lLeftRecord);
           tempRecIndex++;

           while(lIsLeftEmpty = lOutPipeL->Remove(&lLeftRecord)) {
                 lResult = lCompare.Compare(&lLeftRecord, &lLeftOrder, &lRightRecord, &lRightOrder);
                 if( lResult != 0 ) {
                     break;
                 }
                 else {
                    // Merge records and insert into output pipe
                    Record lMergeRecord;
                    int numAttsLeft = ((((int*)(lLeftRecord.bits))[1])/sizeof(int)) - 1;
                    int numAttsRight = ((((int*)(lRightRecord.bits))[1])/sizeof(int)) - 1;
                    int numAttsToKeep = numAttsLeft + numAttsRight;
                    int attsToKeep[numAttsToKeep];
                    int i;
                    for(i = 0; i < numAttsLeft; i++) {
                        attsToKeep[i] = i;
                    }
                    int startOfRight = i;
                    for(int j = 0; j < numAttsRight; j++,i++) {
                        attsToKeep[i] = j;
                   }
                    lMergeRecord.MergeRecords(&lLeftRecord, &lRightRecord, numAttsLeft,
                            numAttsRight, attsToKeep, numAttsToKeep, startOfRight);
                    lJoinObj->mOutPipe->Insert(&lMergeRecord);
                     // store the left record in the temp records array
                     tempRecArray.push_back( new Record );
                     tempRecArray[tempRecIndex]->Copy(&lLeftRecord);
                     tempRecIndex++;
                 }
           }

           while(lIsRightEmpty = lOutPipeR->Remove(&lRightRecord)) {
                 lResult = lCompare.Compare(tempRecArray[0], &lLeftOrder, &lRightRecord, &lRightOrder);
                 if( lResult != 0 ) {
                     break;
                 }
                 else {
                      int k = 0;
                      do {
                         Record mergedRecord;
                         int numAttsLeft = ((((int*)(tempRecArray[k]->bits))[1])/sizeof(int)) - 1;
                         int numAttsRight = ((((int*)(lRightRecord.bits))[1])/sizeof(int)) - 1;
                         int numAttsToKeep = numAttsLeft + numAttsRight;

                         int attsToKeep[numAttsToKeep];
                         int i ;
                         for(i = 0; i < numAttsLeft; i++) {
                            attsToKeep[i] = i;
                         }
                         int startOfRight = i;
                         for(int j = 0; j < numAttsRight; j++,i++) {
                            attsToKeep[i] = j;
                         }
                         mergedRecord.MergeRecords(tempRecArray[k], &lRightRecord, numAttsLeft, numAttsRight, attsToKeep, numAttsToKeep, startOfRight);
                         lJoinObj->mOutPipe->Insert(&mergedRecord);
                         k++;
                      } while(k < tempRecIndex);
                 }
               }
           }
        }
    }
    lJoinObj->mOutPipe->ShutDown();
}

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe){
    mInPipe = &inPipe;
    mOutPipe = &outPipe;
    mFunc = &computeMe;
    pthread_create(&mSumThread, NULL, SumWorker, (void*)this);
}

void* SumWorker(void* iSumObj){
    Record lTempRecord;
    Sum* lSumObj = (Sum*)iSumObj;
    int lIResult, lISum = 0;
    double lDResult, lDSum = 0.0;
    Type lType;
    while(lSumObj->mInPipe->Remove(&lTempRecord)){
        lType = lSumObj->mFunc->Apply(lTempRecord, lIResult, lDResult);
        if(lType == Int) {
               lISum += lIResult;
          }
          else if(lType == Double) {
               lDSum += lDResult;
          }
          else {
               cout<<"Type is not valid\n";
               exit(1);
          }
    }
     char lString[20];
     if(lType == Int) {
          sprintf(lString,"%d|",lISum);
     }
     else if(lType == Double) {
          sprintf(lString,"%.2f|",lDSum);
     }
     Attribute lAttribute = {"double", Double};
     Schema out_sch("out_sch", 1 , &lAttribute);

     Record lRecTemp;
     lRecTemp.ComposeRecord (&out_sch,lString);

     lSumObj->mOutPipe->Insert(&lRecTemp);
     lSumObj->mOutPipe->ShutDown();
}

void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe){
      mInPipe = &inPipe;
      mOutPipe = &outPipe;
      mOrderMaker = &groupAtts;
      mFunc = &computeMe;
      pthread_create(&mGroupByThread, NULL, GroupByWorker, (void*)this);
}
void* GroupByWorker(void* iGroupByObj){
    GroupBy* lGroupByObj = (GroupBy*)iGroupByObj;
    int lGroupNumber=1;
    Pipe lTempPipe(100);
    BigQ lBigQObj(*(lGroupByObj->mInPipe), lTempPipe, *(lGroupByObj->mOrderMaker), lGroupByObj->lRunLength);
    int lIResult, lISum = 0;
    double lDResult, lDSum = 0.0;
    Type lType;
    Record lPrevRecord,lCurrentRecord;
    ComparisonEngine lcomparisonEngine;
    lTempPipe.Remove(&lPrevRecord);
    lType = lGroupByObj->mFunc->Apply(lPrevRecord, lIResult, lDResult);
        if(lType == Int) {
               lISum += lIResult;
          }
          else if(lType == Double) {
               lDSum += lDResult;
          }
          else {
               cout<<"Type is not valid\n";
               exit(1);
          }
    while(lTempPipe.Remove(&lCurrentRecord)){
      if(lcomparisonEngine.Compare(&lCurrentRecord,&lPrevRecord,lGroupByObj->mOrderMaker)){
        cout<<"not equal"<<endl;
        char lString[20];
        if(lType == Int) {
              sprintf(lString,"%d|",lISum);
         }
          else if(lType == Double) {
              sprintf(lString,"%.2f|",lDSum);
           }
           cout<<"Group Number " << lGroupNumber << " sum is : " << lString<<endl;
           Attribute lAttribute = {"double", Double};
           Schema out_sch("out_sch", 1 , &lAttribute);

            Record lRecTemp;
            lRecTemp.ComposeRecord (&out_sch,lString);
            lGroupByObj->mOutPipe->Insert(&lRecTemp);

            lPrevRecord.Consume(&lCurrentRecord);
            lISum = 0;lDSum = 0.0;;
            lType = lGroupByObj->mFunc->Apply(lPrevRecord, lIResult, lDResult);
        if(lType == Int) {
               lISum += lIResult;
          }
          else if(lType == Double) {
               lDSum += lDResult;
          }
          else {
               cout<<"Type is not valid\n";
               exit(1);
          }

            lGroupNumber++;
       }//if close
       else{
        cout<<"equal"<<endl;
          lType = lGroupByObj->mFunc->Apply(lCurrentRecord, lIResult, lDResult);
        if(lType == Int) {
               lISum += lIResult;
          }
          else if(lType == Double) {
               lDSum += lDResult;
          }
          else {
               cout<<"Type is not valid\n";
               exit(1);
          }
       }//end of else
    }//end of while
    char lString[20];
        if(lType == Int) {
              sprintf(lString,"%d|",lISum);
         }
          else if(lType == Double) {
              sprintf(lString,"%.2f|",lDSum);
           }
           cout<<"Group Number " << lGroupNumber << " sum is : " << lString<<endl;
           Attribute lAttribute = {"double", Double};
           Schema out_sch("out_sch", 1 , &lAttribute);

            Record lRecTemp;
            lRecTemp.ComposeRecord (&out_sch,lString);
            lGroupByObj->mOutPipe->Insert(&lRecTemp);
            lGroupNumber++;

            lGroupByObj->mOutPipe->ShutDown();
} 


void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema){
    mInPipe = &inPipe;
    mOutPipe = &outPipe;
    mSchema = &mySchema;
    pthread_create(&mDupRemovalThread, NULL, DuplicateRemovalWorker, (void*)this);
}

void* DuplicateRemovalWorker(void* iDupObj){
    DuplicateRemoval* lDupObj = (DuplicateRemoval*)iDupObj;
    int lRunLength;
    OrderMaker lSortOrderObj(lDupObj->mSchema);
    Pipe lTempPipe(100);
    BigQ lBigQObj(*(lDupObj->mInPipe), lTempPipe, lSortOrderObj, lRunLength);
    ComparisonEngine lcomparisonEngine;
    Record lPrevRecord;
    Record lCurrentRecord;
    lTempPipe.Remove(&lPrevRecord);
    while(lTempPipe.Remove(&lCurrentRecord)){
        if(lcomparisonEngine.Compare(&lCurrentRecord,&lPrevRecord,&lSortOrderObj)){
            lDupObj->mOutPipe->Insert(&lPrevRecord);
            lPrevRecord.Consume(&lCurrentRecord);
        }
    }
    lDupObj->mOutPipe->Insert(&lPrevRecord);
    lDupObj->mOutPipe->ShutDown();
}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema){
    mInPipe = &inPipe;
    mOutFile = outFile;
    mMySchema = &mySchema;
    pthread_create(&mWriteOutThread, NULL, WriteOutWorker, (void*)this);
}

void* WriteOutWorker(void* iWriteOutObj){
  WriteOut* lWriteOutObj = (WriteOut*)iWriteOutObj;
  Record lTempRecord;
  while(lWriteOutObj->mInPipe->Remove(&lTempRecord)){
    int lNumOfAttr = lWriteOutObj->mMySchema->GetNumAtts();
    Attribute *lAttr = lWriteOutObj->mMySchema->GetAtts();
    for (int i = 0; i < lNumOfAttr; i++) {
      fprintf(lWriteOutObj->mOutFile, "%s%s",lAttr[i].name,":[");
      int lRecordValue = ((int *) lTempRecord.bits)[i + 1];
      if (lAttr[i].myType == Int) {
          int *mInt = (int *) &(lTempRecord.bits[lRecordValue]);
          fprintf(lWriteOutObj->mOutFile, "%d", *mInt);
      }
      else if (lAttr[i].myType == Double) {
          double *mDouble = (double *) &(lTempRecord.bits[lRecordValue]);
          fprintf(lWriteOutObj->mOutFile, "%f", *mDouble);
      }
      else if (lAttr[i].myType == String) {
           char *mString = (char *) &(lTempRecord.bits[lRecordValue]);
           fprintf(lWriteOutObj->mOutFile, "%s", *mString);
      }
      fprintf(lWriteOutObj->mOutFile, "%s", "]");
      if (i != lNumOfAttr - 1) {
            fprintf(lWriteOutObj->mOutFile, "%s", ",");
            }
      }
      fprintf(lWriteOutObj->mOutFile, "\n");

  }
  fclose(lWriteOutObj->mOutFile);
}