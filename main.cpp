/*
* test.c
* Zhaonian Zou
* Harbin Institute of Technology
* Jun 22, 2011
*/
#include <string>
#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include "extmem.h"
#include "BufferManager.h"
#include "RawTable.h"
#include "Table.h"
#include "Index.h"
using namespace std;
void testTable();
void testIndex();
int main(int argc, char **argv)
{
	for (unsigned i = 1; i < 300; ++i)
		dropBlockOnDisk(i);
	system("pause");
	return 0;
}
void testIndex()
{
	FIFOBufMgr buf(3 * 21, 21);
	HashIndex index(buf, 4, false, 4);
	unsigned size = 30;
	unsigned char attr[4];
	for (unsigned i = 1; i < size; ++i)
	{
		*(unsigned*)attr = i;
		index.insert(attr, i, i);
		cout << "insert " << i << endl;
	}
	for (unsigned i = 1; i < size; ++i)
	{
		*(unsigned*)attr = i;
		auto res = index.get(attr);
		cout << "res of "<< i << " : ";
		for (auto entry : res)
		{
			cout << "<" << entry.first << ", " << entry.second << "> ";
		}
		cout << endl;
	}
	for (unsigned i = 1; i < size; i+=2)
	{
		*(unsigned*)attr = i;
		index.remove(attr, i, i);
	}
	for (unsigned i = 1; i < size; ++i)
	{
		*(unsigned*)attr = i;
		auto res = index.get(attr);
		cout << "res of "<< i << " : ";
		for (auto entry : res)
		{
			cout << "<" << entry.first << ", " << entry.second << "> ";
		}
		cout << endl;
	}
}
void testMoreRawTable()
{
	FIFOBufMgr buf(3 * (1 + 8 + 12), 21);
	RawTable t(buf, 4, false);
	unsigned char tuple[4];
	for (unsigned i = 1; i < 20; i+=2)
	{
		*(unsigned*)tuple = i;
		auto pos = t.linearSerach(tuple, 0, 4);
		t.rawAdd(tuple, pos.second.first, pos.second.second);
	}
	for (unsigned i = 1; i < 20; i+=2)
	{
		*(unsigned*)tuple = i;
		auto pos = t.linearSerach(tuple, 0, 4);
		cout << "<" << pos.first << ", " << pos.second.first << ", " << pos.second.second << ">\n";
	}
	for (unsigned i = 2; i < 20; i+=2)
	{
		*(unsigned*)tuple = i;
		auto pos = t.linearSerach(tuple, 0, 4);
		t.rawAdd(tuple, pos.second.first, pos.second.second);
	}/*
	for (unsigned i = 3; i < 20; i+=3)
	{
		*(unsigned*)tuple = i;
		auto pos = t.linearSerach(tuple, 0, 4);
		t.rawAdd(tuple, pos.second.first, pos.second.second);
	}*/
	for (unsigned i = 1; i < 20; ++i)
	{
		*(unsigned*)tuple = i;
		auto pos = t.linearSerach(tuple, 0, 4);
		cout << "<" << pos.first << ", " << pos.second.first << ", " << pos.second.second << ">\n";
	}
}
void testTable()
{
	FIFOBufMgr buf(8 * (1 + 8 + 8*2), 8 + 8*2);
	vector<unsigned> v = { 0, 4, 8 };
	SeqTable t(buf, 8, 2, v, false);
	unsigned char first[4], second[4];
	unsigned size = 30;
	for (unsigned i = 1; i < size; i += 4) {
		*(unsigned*)first = i;
		*(unsigned*)second = i;
		//cout << "insert " << i << endl;
		t.insert({ { 1,first },{ 2,second } });
	}
	for (unsigned i = 2; i < size; i += 4) {
		*(unsigned*)first = i;
		*(unsigned*)second = i;
		//cout << "insert " << i << endl;
		t.insert({ { 1,first },{ 2,second } });
	}
	for (unsigned i = 3; i < size; i += 4) {
		*(unsigned*)first = i;
		*(unsigned*)second = i;
		//cout << "insert " << i << endl;
		t.insert({ { 1,first },{ 2,second } });
	}
	for (unsigned i = 4; i < size; i += 4) {
		*(unsigned*)first = i;
		*(unsigned*)second = i;
		//cout << "insert " << i << endl;
		t.insert({ { 1,first },{ 2,second } });
	}
	for (unsigned i = 1; i < size; i += 4) {
		*(unsigned*)first = i;
		*(unsigned*)second = i;
		//cout << "insert " << i << endl;
		t.insert({ { 1,first },{ 2,second } });
	}
	for (unsigned i = 2; i < size; i += 4) {
		*(unsigned*)first = i;
		*(unsigned*)second = i;
		//cout << "insert " << i << endl;
		t.insert({ { 1,first },{ 2,second } });
	}
	for (unsigned i = 3; i < size; i += 4) {
		*(unsigned*)first = i;
		*(unsigned*)second = i;
		//cout << "insert " << i << endl;
		t.insert({ { 1,first },{ 2,second } });
	}
	for (unsigned i = 4; i < size; i += 4) {
		*(unsigned*)first = i;
		*(unsigned*)second = i;
		//cout << "insert " << i << endl;
		t.insert({ { 1,first },{ 2,second } });
	}
	for (unsigned i = 1; i < size; ++i)
	{
		unsigned char tuple[4];
		*(unsigned*)tuple = i;
		auto p = t.binarySearch(tuple);
		//if (p[0] != 0)
		//cout << i << endl;
		cout << i << ": " << (int)p[0] << "," << p[1] << "," << p[3] << endl;
	}
	
	for (unsigned i = 1; i < size; i+=5)
	{
		*(unsigned*)second = i;
		t.remove({ {1, second} });
	}
	
	for (unsigned i = 1; i < size; ++i)
	{
		unsigned char tuple[4];
		*(unsigned*)tuple = i;
		auto res = t.select({ {2, tuple} });
		for (auto j : res)
		{
			cout << "res of " << i << ": " << *(unsigned*)(j + 4) << endl;
		}
		//auto p = t.binarySearch(tuple);
		//if (p[0] != 0)
			//cout << i << endl;
		//cout << i << ": " << (int)p[0] << "," << p[1] << "," << p[3] << endl;
	}
	
}
/*
void testSearch()
{
	FIFOBufMgr buf(3 * (1 + 8 + 4 * 3), 8 + 4 * 3);
	RawTable tb(buf, 4, false);
	for (unsigned i = 1; i < 20; i++)
	{
		unsigned char tuple[8];
		*(unsigned*)tuple = i;
		auto res = tb.linearSerach(tuple, 0, 4);
		//cout << "before insert pos:" << res.second.first << ", " << res.second.second << endl;
		tb.rawAdd(tuple, res.second.first, res.second.second);
	}
	/*
	for (unsigned i = 2; i < 200; i += 2)
	{
		unsigned char tuple[8];
		*(unsigned*)tuple = i;
		auto res = tb.linearSerach(tuple, 0, 4);
		cout << i << ": before insert pos:" << res.second.first << ", " << res.second.second << endl;
		tb.rawAdd(tuple, res.second.first, res.second.second);
		
		/*res = tb.linearSerach(tuple, 0, 4);
		cout << "tuple " << i << " linear search result <" << res.first << ", <" << res.second.first << ", " << res.second.second << ">>" << endl;
		res = tb.linearSerach(tuple, 0, 4);
		cout << "tuple " << i << " binary search result <" << res.first << ", <" << res.second.first << ", " << res.second.second << ">>" << endl;
		*/
/*	}
	for (unsigned i = 1; i < 20; ++i) {
		unsigned char tuple[8];
		*(unsigned*)tuple = i;
		auto res = tb.linearSerach(tuple, 0, 4);
		cout << "tuple " << i << " linear search result <" << res.first << ", <" << res.second.first << ", " << res.second.second << ">>" << endl;
		//res = tb.binarySearch(tuple, 0, 4);
		//cout << "tuple " << i << " binary search result <" << res.first << ", <" << res.second.first << ", " << res.second.second << ">>" << endl;
	}
	cout << "-----------------" << endl;
	for (unsigned i = 2; i < 20; i += 2)
	{
		unsigned char tuple[8];
		*(unsigned*)tuple = i;
		tb.rawRemove(tuple, 0, 4);
	}
	for (unsigned i = 1; i < 20; ++i) {
		unsigned char tuple[8];
		*(unsigned*)tuple = i;
		auto res = tb.linearSerach(tuple, 0, 4);
		cout << "tuple " << i << " linear search result <" << res.first << ", <" << res.second.first << ", " << res.second.second << ">>" << endl;
		//res = tb.binarySearch(tuple, 0, 4);
		//cout << "tuple " << i << " binary search result <" << res.first << ", <" << res.second.first << ", " << res.second.second << ">>" << endl;
	}
}
*/
/*
void testRawTable()
{
	FIFOBufMgr bm(30 * 17, 8 + 8);
	RawTable table(bm, 4, false);
	unsigned char tuple[4] = { 0 };
	for (int i = 1; i <= 5; ++i)
	{
		*(unsigned*)tuple = i;
		table.rawAdd(tuple);

	}
	for (int i = 1; i <= 5; ++i)
	{
		*(unsigned*)tuple = i;
		vector<unsigned char*> res = table.rawSelect(tuple, 0, 4);
		for (auto i : res)
		{
			printf("read %d\n", *(unsigned*)i);
		}
	}
	cout << "------------\n";
	*(unsigned*)tuple = 3;
	unsigned char after[4];
	*(unsigned*)after = 6666;
	table.rawUpdate(tuple, 0, 4, after);
	for (int i = 1; i <= 5; ++i)
	{
		*(unsigned*)tuple = (i == 3) ? 6666 : i;
		vector<unsigned char*> res = table.rawSelect(tuple, 0, 4);
		for (auto i : res)
		{
			printf("read %d\n", *(unsigned*)i);
		}
	}
	cout << "--------------\n";
	*(unsigned*)tuple = 2;
	table.rawRemove(tuple, 0, 4);
	*(unsigned*)tuple = 1;
	table.rawRemove(tuple, 0, 4);
	for (int i = 1; i <= 5; ++i)
	{
		*(unsigned*)tuple = i;
		vector<unsigned char*> res = table.rawSelect(tuple, 0, 4);
		for (auto i : res)
		{
			printf("read %d\n", *(unsigned*)i);
		}
	}
}
*/
void testBM()
{
	LRUBufMgr bm(3 * 9, 8);
	for (unsigned i = 1; i <= 5; ++i)
	{
		unsigned char* wr = bm.get(i);
		*(unsigned*)wr = i;
		cout << "write " << i << " into block " << i << endl;
		if (!bm.write(i))
		{
			cout << "WRITE ERROR " << i << endl;
		}
	}
	cout << "# of IO: " << bm.getNumIO() << endl;
	for (unsigned i = 5; i >= 1; --i)
	{
		unsigned char *rd = bm.read(i);
		cout << "read " << *(unsigned*)rd << " from block " << i << endl;
	}
	cout << "# of IO: " << bm.getNumIO() << endl;
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
