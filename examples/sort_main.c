#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "merge.h"
#include <math.h>

#define RECORDS_NUM 100 // you can change it if you want
#define FILE_NAME "data.db"
#define OUT_NAME "out"

int createAndPopulateHeapFile(char *filename);

void sortPhase(int file_desc, int chunkSize);

void mergePhases(int inputFileDesc, int chunkSize, int bWay, int *fileCounter);

int nextOutputFile(int *fileCounter);

int main()
{
  int chunkSize, bWay, fileIterator;

  BF_Init(LRU);
  int file_desc = createAndPopulateHeapFile(FILE_NAME);
  chunkSize = 6;
  bWay = round(RECORDS_NUM / (chunkSize * HP_GetMaxRecordsInBlock(file_desc))) + 1;
  fileIterator = 0;

  sortPhase(file_desc, chunkSize);
  printf("FINAL -----------------------------------\n\n");
  mergePhases(file_desc, chunkSize, bWay, &fileIterator);
  printAllRecords(file_desc);
}

int createAndPopulateHeapFile(char *filename)
{
  HP_CreateFile(filename);

  int file_desc;
  HP_OpenFile(filename, &file_desc);

  Record record;
  srand(12569874);
  for (int id = 0; id < RECORDS_NUM; ++id)
  {
    record = randomRecord();
    HP_InsertEntry(file_desc, record);
  }
  return file_desc;
}

/*Performs the sorting phase of external merge sort algorithm on a file specified by 'file_desc', using chunks of size 'chunkSize'*/
void sortPhase(int file_desc, int chunkSize)
{
  sort_FileInChunks(file_desc, chunkSize);
}

/* Performs the merge phase of the external merge sort algorithm  using chunks of size 'chunkSize' and 'bWay' merging. The merge phase may be performed in more than one cycles.*/
void mergePhases(int inputFileDesc, int chunkSize, int bWay, int *fileCounter)
{
  int oututFileDesc;
  while (chunkSize <= HP_GetIdOfLastBlock(inputFileDesc))
  {
    oututFileDesc = nextOutputFile(fileCounter);
    merge(inputFileDesc, chunkSize, bWay, oututFileDesc);
    HP_PrintAllEntries(oututFileDesc);
    HP_CloseFile(inputFileDesc);
    exit(-1);
    chunkSize *= bWay;
    inputFileDesc = oututFileDesc;
  }
  HP_CloseFile(oututFileDesc);
}

/*Creates a sequence of heap files: out0.db, out1.db, ... and returns for each heap file its corresponding file descriptor. */
int nextOutputFile(int *fileCounter)
{
  char mergedFile[50];
  char tmp[] = "out";
  sprintf(mergedFile, "%s%d.db", tmp, (*fileCounter)++);
  int file_desc;
  HP_CreateFile(mergedFile);
  HP_OpenFile(mergedFile, &file_desc);
  return file_desc;
}
