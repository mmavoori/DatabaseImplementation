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

// Algorithm used in filling and commiting the write buffer: (this handles all
// combinations of Add and GetNext so that there are no gaps in the pages of the file)
//
// The flag 'writeBufferCopyOfLastPage' tells if the write buffer is a copy of 
// the file's last page or a fresh buffer.
//
// Filling write buffer:
//         case1: with last page of file --> set writeBufferCopyOfLastPage = 1
//         case2: a fresh buffer         --> set writeBufferCopyOfLastPage = 0
//
// Commiting write buffer:
//         if(writeBufferCopyOfLastPage)
//                commit to last existing page of file by overwriting it
//         else
//                commit to a fresh page after the existing last page of file

DBFile::DBFile () 
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
    
    writeBufferDirty = 0;
    curReadPageIndex = -2; // file.GetLength() starts at 0 and takes values 2,3,4... & not 1
                           // Actually it should start at 1 and take values 2,3,4... 
                           // (Confusing@#!$* bcoz 'O'th page has no data in a file)
}

int DBFile::Create (char *file_path, fType file_type, void *startup) 
{
    // for this assignment
    file_type = heap;
    
    file.Open(0,file_path);
    return 1;
}

int DBFile::Open (char *file_path) 
{
    file.Open(1,file_path);
    
    // Get the first page of the file, if any, into the read buffer
    if(file.GetLength() > 1) {   // means not an empty file
         curReadPageIndex = 0;
         file.GetPage(readPageBuffer, curReadPageIndex);
         recordIndex = 0;
    }
    
    // Get the last page of the file, if any, into the write buffer, because
    // we have to append records to the file without leaving any spaces.
    if(file.GetLength() > 1) {   // means not an empty file  
         file.GetPage(writePageBuffer, file.GetLength() - 2);
         writeBufferCopyOfLastPage = 1;
    }
    
    return 1;
}

int DBFile::Close () 
{
    // write to disk any unwritten buffer
    // Almost always write buffer is dirty, except in the cases where you commit
    // non-full write buffer and where you close the 
    // file without writing even a single record to the buffer
    if(writeBufferDirty) {
        file.AddPage(writePageBuffer,file.GetLength());
        writePageBuffer->EmptyItOut();
        writeBufferDirty = 0;
    }
    
    int temp_file_pageLength = file.Close();
    return 1;
}


void DBFile::Load (Schema &f_schema, char *load_file_path) 
{
     FILE *file = fopen(load_file_path,"r");
     if(file == NULL) 
     {
             cerr<< "Opening file \""<< load_file_path <<"\" failed.\n";
             exit(1);
     }
     
     // Suck each record from the load file and add it to write buffer
     Record temp;
	 while (temp.SuckNextRecord(&f_schema, file)) {
		Add(temp);
	 }
	 
	 fclose(file);
}

void DBFile::MoveFirst () 
{
     // Get the first page of the file into the read buffer
     readPageBuffer->EmptyItOut();
     file.GetPage(readPageBuffer,0);
     recordIndex = 0;
     curReadPageIndex = 0;
}

void DBFile::Add (Record &addme) 
{
     // add the record to the write buffer
     int appended = writePageBuffer->Append(&addme);
     
     if(!appended) {
          // write buffer is full, so write it to the disk
          
          // Overwrite case1: We have to overwrite the last page of the file during 
          // the first commit of a write buffer, since we got a copy of the file's 
          // last page and made additions to it.
          // file.GetLength - 2 gives the index of the last page of the file.
          if(writeBufferCopyOfLastPage == 1) {
              file.AddPage(writePageBuffer,file.GetLength());
          }
          else { 
               // Freshwrite allOtherCases: commit to a fresh page after last page.
              file.AddPage(writePageBuffer,file.GetLength());
          }
          
          // now add the record to a fresh buffer
          writePageBuffer->EmptyItOut();
          writeBufferCopyOfLastPage = 0;
          appended = writePageBuffer->Append(&addme);
     }
     
     writeBufferDirty = 1;
}

int DBFile::GetNext (Record &fetchme) 
{
    // TODO: For mixed read writes implement the below commented out code.
    
    // write buffer is not full, but we write it to the disk
    //
    // Overwrite case2: If we are reading from the last page of the file, first 
    // make sure you commit any unwritten write buffer to the disk's last page
    // and read back the last page and skip all fetched records
    /*if((curReadPageIndex == file.GetLength() - 2) && writeBufferDirty == 1) {
          // commit dirty write buffer
          if(writeBufferCopyOfLastPage == 1) {
              file.AddPage(writePageBuffer,file.GetLength() - 1);
          }
          else { 
               // Freshwrite allOtherCases: commit to a fresh page after last page.
              file.AddPage(writePageBuffer,file.GetLength() - 1);
          }
          // last page is a copy of the write buffer, which means effectively
          // still write buffer is a copy of the last page
          writeBufferCopyOfLastPage = 1;
          writeBufferDirty = 0;
          
          // read back commited last page
          readPageBuffer->EmptyItOut();
          file.GetPage(readPageBuffer, file.GetLength() - 2);
          recordIndex = 0;
          
          // skip all fetched records
          Record *temp;
          for (int i = 0; i < recordIndex; i++) {
			readPageBuffer->GetFirst(temp);
		  }
    }*/
          
    // get the next record from the read buffer
    int got_next = readPageBuffer->GetFirst(&fetchme);
    
    if(!got_next) {
          // read buffer is empty
          // Check if this is the last page of the file
          if(curReadPageIndex == (file.GetLength() - 2)) {
               return 0;  // finished reading the entire file
          }
          else { 
               // read next page from the disk
               readPageBuffer->EmptyItOut();
               file.GetPage(readPageBuffer, ++curReadPageIndex);
               recordIndex = 0;
             
               // now get the next record from the read buffer
               got_next = readPageBuffer->GetFirst(&fetchme);
          }
    }
    
    recordIndex++;
    return 1;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) 
{
    ComparisonEngine comparisionEngine;
	
	// Get each record and compare
	do {
		if (GetNext(fetchme) == 0) {
			return 0;  // finished reading the entire file
		}
	} while (!comparisionEngine.Compare(&fetchme, &literal, &cnf));
	
	return 1;
}
