#include <string>
#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include "extmem.h"
#include "BufferManager.h"
#include "RawTable.h"
#include "Table.h"
#include "Index.h"
#include "DB.h"
using namespace std;
void testDB();
int main(int argc, char **argv)
{
	testDB();
	system("pause");
	return 0;
}
void doDB(DB& db, vector<unsigned> v)
{
	unsigned char cond[4];
	for (auto i : v)
	{
		*(unsigned*)cond = i;
		db.insertInto("A", { {1,cond},{2,cond} }, false);
	}
}
void rmvDB(DB& db, vector<unsigned> v)
{
	unsigned char cond[4];
	for (auto i : v)
	{
		*(unsigned*)cond = i;
		db.removeFrom("A", { { 2,cond }});
	}
}
void selDB(DB& db, vector<unsigned> v)
{
	unsigned char cond[4];
	for (auto i : v)
	{
		*(unsigned*)cond = i;
		cout << "sel of " << i << ": ";
		auto res = db.selectFrom("A", { { 1,cond } });
		for (auto j : res)
		{
			cout << "<" << *(unsigned*)j << ", " << *(unsigned*)(j + 4) << "> ";
		}
		cout << endl;
	}
}

void testDB()
{
	FIFOBufMgr buf(30 * 21, 21);
	DB db(buf);
	
	db.createSeqTable("A", 1, vector<unsigned>({ 0, 4, 8 }), false);
	auto& index = db.createHashIndexOn("IA", "A", 1, 4);
	unsigned size = 30;
	unsigned char cond[4];
	vector<unsigned> v;
	for (unsigned i = 1; i < size; i+=4)v.push_back(i);
	for (unsigned i = 2; i < size; i += 4)v.push_back(i);
	for (unsigned i = 3; i < size; i += 4)v.push_back(i);
	for (unsigned i = 4; i < size; i += 4)v.push_back(i);
	doDB(db, v);
	selDB(db, v);
	v.clear();
	for (unsigned i = 1; i < size; i += 3)v.push_back(i);
	for (unsigned i = 2; i < size; i += 3)v.push_back(i);
	rmvDB(db, v);
	v.clear();
	for (unsigned i = 1; i < size; ++i)v.push_back(i);
	selDB(db, v);
}
int demo()
{
	Buffer buf; /* A buffer */
	unsigned char *blk; /* A pointer to a block */
	int i = 0;

	/* Initialize the buffer */
	if (!initBuffer(20, 8, &buf))
	{
		perror("Buffer Initialization Failed!\n");
		return -1;
	}

	/* Get a new block in the buffer */
	blk = getNewBlockInBuffer(&buf);

	/* Fill data into the block */
	for (i = 0; i < 2; i++)
		*(blk + i * 4) = i + 100;

	/* Write the block to the hard disk */
	if (writeBlockToDisk(blk, 31415926, &buf) != 0)
	{
		perror("Writing Block Failed!\n");
		return -1;
	}

	/* Read the block from the hard disk */
	if ((blk = readBlockFromDisk(31415926, &buf)) == NULL)
	{
		perror("Reading Block Failed!\n");
		return -1;
	}

	for (i = 0; i < 2; ++i)
		printf("%d\n", *(blk + i * 4));

	/* Process the data in the block */
	for (i = 0; i < 8; i++)
		printf("%c", *(blk + i));

	printf("\n");
	printf("# of IO's is %d\n", buf.numIO); /* Check the number of IO's */
}