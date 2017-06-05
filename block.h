/*
数据磁盘块结构：
4字节unsigned：当前起始的空闲部分相对于磁盘块的偏移地址（初始化为8）
4字节unsigned：下一磁盘块地址（-1表示无下一地址）
余下部分：数据
*/

/*
meta磁盘块链表结构：
4字节unsigned：tupleSize
4字节unsigned：startAddr
4字节unsigned：numBlk
1字节bool：cmp
---------------------above: RawTable
3字节放空
4字节unsigned：bucketSize
---------------------above: HashIndex

4字节unsigned：colPk
4字节unsigned：colNum
随后：每4个字节保存一列所占的字节数

随后的numBlk个4字节：存储对应的index
---------------------above: SeqTable
*/