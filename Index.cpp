#include "Index.h"

Index::Index(BufferManager &_bm, unsigned _metaAddr) :SeqTable(_bm, _metaAddr)
{
	attrSize = cols[1];
}

Index::Index(BufferManager & _bm, unsigned _attrSize, bool _cmp)
	: SeqTable(_bm, _attrSize + 8, 1, std::vector<unsigned>({ 0, _attrSize, _attrSize + 4, _attrSize + 8 }), _cmp), attrSize(_attrSize)
{
}

Index::~Index()
{
	delete[] icache;
}

HashIndex::HashIndex(BufferManager & _bm, unsigned _metaAddr):Index(_bm, _metaAddr)
{
	unsigned char* blk = _bm.read(_metaAddr);
	bucketSize = *(unsigned*)(blk + 24);
}

HashIndex::HashIndex(BufferManager & _bm, unsigned _attrSize, bool _cmp, unsigned _bucketSize)
	:Index(_bm, _attrSize, _cmp), bucketSize(_bucketSize)
{
	//初始化桶
	index.resize(bucketSize);
	for (unsigned i = 1; i < bucketSize; ++i)
	{
		unsigned addr = getFreeBlockOnDisk();
		unsigned char* blk = bm.get(addr);
		*(unsigned*)blk = 8;
		*(unsigned*)(blk + 4) = -1;
		bm.write(addr);
		++numBlk;
		//更新索引
		index[i] = addr;
	}
}

std::vector<std::pair<unsigned, unsigned>> HashIndex::get(unsigned char * attr)
{
	std::vector<std::pair<unsigned, unsigned>> res;

	std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> cond{ {attr, {0, attrSize}} };
	unsigned char *blk;
	for (unsigned addr = index[hash(attr)]; addr != -1; addr = getNextAddr(blk))
	{
		auto tmp = doSelect(blk, cond, addr);
		for (auto tuple : tmp)
		{
			res.emplace_back(*(unsigned*)(tuple + attrSize), *(unsigned*)(tuple + attrSize + 4));
		}
		/*
		原始版本
		blk = bm.read(addr);
		for (unsigned char* p = blk + 8; p != *(unsigned*)blk + blk; p += tupleSize)
		{
			if (memcmp(p, attr, attrSize) == 0)
			{
				res.push_back({ *(unsigned*)(p + attrSize), *(unsigned*)(p + attrSize + 4) });
			}
		}
		*/
	}
	return res;
}

int HashIndex::insert(unsigned char * attr, unsigned addr, unsigned offset)
{
	unsigned bucket = index[hash(attr)];
	unsigned char rawAddr[4], rawOffset[4];
	*(unsigned*)rawAddr = addr;
	*(unsigned*)rawOffset = offset;
	format(icache, { {1, attr}, {2, rawAddr}, {3, rawOffset} });
	rawAdd(icache, bucket, 8);
	return 1;
}

int HashIndex::remove(unsigned char * attr, unsigned addr, unsigned offset)
{
	unsigned bucket = index[hash(attr)];
	unsigned char rawAddr[4], rawOffset[4];
	*(unsigned*)rawAddr = addr;
	*(unsigned*)rawOffset = offset;
	format(icache, { { 1, attr },{ 2, rawAddr },{ 3, rawOffset } });
	int res = 0;
	for (unsigned char* blk; !(res = doRemove(blk, { {icache, {0, tupleSize}} }, bucket))
		&& (bucket = getNextAddr(blk)) != -1;);
	return res;
}

unsigned HashIndex::hash(unsigned char * attr)
{
	unsigned res = 0;
	unsigned *p = (unsigned*)attr;
	for (unsigned i = 0; i < attrSize / 4; ++i)
		res += *p++;
	return res % bucketSize;
}
