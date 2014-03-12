
#ifndef _HIGH_HASH_MANAGEMENT_H_
#define _HIGH_HASH_MANAGEMENT_H_

#include "General.h"

namespace exth
{
	class HighHashManagement
	{
	private:

		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	哈希索引分裂，并将原桶指针复制给分裂桶，同时全局深度加一
		void hashIndexSplit()
		{
			//高位中哈希索引先分裂，原来一个节点分裂成两个连续的节点，分裂后的两个节点的对应桶与原节点相同


			//先确定有要复制的哈希索引节点的起点和终点
			HashIndex fromHashIndex = HashIndex(*_dbController, 0);
			fromHashIndex.Pin();
			int initialHashIndexPages = _hashIndexPages;
			int initialNumInLastPage = _numInLastPage;
			fromHashIndex.Save();
			fromHashIndex.Unpin();

			//插入新哈希索引，每个索引的桶偏移值为-1
			HashIndex toHashIndex = HashIndex(*_dbController, _hashIndexPages);
			toHashIndex.Pin();

			int NodeID = (int)pow(2.0, _globalDepth);
			int insertNum = (int)pow(2.0, _globalDepth);

			for(int i=0; i<insertNum; i++)
			{
				//获得将要插入的节点
				indexNode temp = indexNode(NodeID, -1);
				NodeID++;

				if(toHashIndex.insertIndex(temp) == false)
				{
					toHashIndex.Save();
					toHashIndex.Unpin();
					toHashIndex = HashIndex(*_dbController);
					toHashIndex.Pin();
					_hashIndexPages++;

					toHashIndex.insertIndex(temp);
					_numInLastPage = 1;
				}
				else
				{
					_numInLastPage++;
				}
			}
			_globalDepth++;
			toHashIndex.Save();
			toHashIndex.Unpin();


			//修改哈希索引节点的桶偏移值属性
			int fromPageNum = initialHashIndexPages;
			int fromNumInLastPage = initialNumInLastPage;
			fromHashIndex = HashIndex(*_dbController, fromPageNum);
			fromHashIndex.Pin();

			int toPageNum = _hashIndexPages;
			int toNumInLastPage = _numInLastPage;
			toHashIndex = HashIndex(*_dbController, toPageNum);
			toHashIndex.Pin();

			indexNode *fromIndexNode;
			indexNode *firstToIndexNode;
			indexNode *secondToIndexNode;
			for(int i=0; i<insertNum; i++)
			{
				//当前页剩余节点数为零
				if(fromNumInLastPage == 0)
				{
					fromPageNum--;
					fromNumInLastPage = _maxNumPerPage;

					fromHashIndex.Save();
					fromHashIndex.Unpin();
					fromHashIndex = HashIndex(*_dbController, fromPageNum);
					fromHashIndex.Pin();
				}
				fromIndexNode = fromHashIndex.getIndexNode(fromNumInLastPage);
				fromNumInLastPage--;

				//
				if(toNumInLastPage == 0)
				{
					toPageNum--;
					toNumInLastPage = _maxNumPerPage;

					toHashIndex.Save();
					toHashIndex.Unpin();
					toHashIndex = HashIndex(*_dbController, toPageNum);
					toHashIndex.Pin();
				}
				secondToIndexNode = toHashIndex.getIndexNode(toNumInLastPage);
				secondToIndexNode->setBucketOffset(fromIndexNode->getBucketOffset());
				toHashIndex.Save();
				toNumInLastPage--;

				//
				if(toNumInLastPage == 0)
				{
					toPageNum--;
					toNumInLastPage = _maxNumPerPage;

					toHashIndex.Save();
					toHashIndex.Unpin();
					toHashIndex = HashIndex(*_dbController, toPageNum);
					toHashIndex.Pin();
				}
				firstToIndexNode = toHashIndex.getIndexNode(toNumInLastPage);
				toNumInLastPage--;

				firstToIndexNode->setBucketOffset(fromIndexNode->getBucketOffset());
			}

			fromHashIndex.Save();
			fromHashIndex.Unpin();
			toHashIndex.Save();
			toHashIndex.Unpin();
		}

		// @Parameters:	index 分裂桶的初始地址，localDepth 原桶的局部深度
		// @Return:		(void)
		// @Function:	将所有应该指向分裂桶的索引节点重新定义
		void reAssignIndex(int index, int localDepth)
		{
			int splitIndex = index;
			int reAssignNum = (int)pow(2.0, _globalDepth - localDepth - 1);
			for(int i=0; i<reAssignNum; i++)
			{
				//修改分裂桶对应的索引节点指针
				int toOpen = splitIndex / _maxNumPerPage;
				HashIndex fromHashIndex = HashIndex(*_dbController, toOpen);
				fromHashIndex.Pin();
				fromHashIndex.getIndexNode(splitIndex % _maxNumPerPage + 1)->setBucketOffset(_bucketNum);
				fromHashIndex.Save();
				fromHashIndex.Unpin();

				splitIndex++;
			}
		}

		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	在桶分裂时重新分配桶中的记录,并刷新局部深度
		void reAssign(Bucket &from, Bucket &split)
		{
			int slotNumber = from.getslotNumber();
			int localDepth = from.getLocalDepth();

			int bucketID = from.getBucketID();
			from.setBucketID(2 * bucketID);
			split.setBucketID(bucketID * 2 + 1);

			for(int i=1; i<=slotNumber; i++)
			{
				Record temp = from.getRecord(i);
				if(hashFunc(temp.getOrderKey(), localDepth + 1) != from.getBucketID())
				{
					from.remove(i);
					split.insert(temp);
				}
			}

			from.resort();
			from.setlocalDepth(localDepth + 1);
			split.setlocalDepth(localDepth + 1);
		}

		// @Parameters:	key 需要插入的记录的键值
		// @Return:		返回该键值哈希后的第一个桶偏移量
		// @Function:	根据传入的键值返回对应桶的偏移量
		int getCorrespondBucket(int key)
		{
			//找出相应哈希索引节点所在页
			int toOpen = key / _maxNumPerPage;
			HashIndex fromHashIndex = HashIndex(*_dbController, toOpen);
			fromHashIndex.Pin();

			//找出所指向的桶的偏移值
			indexNode *hashIndexNode = fromHashIndex.getIndexNode(key % _maxNumPerPage + 1);	//参数从一开始，所以传参时要加一
			int bucketOffset = hashIndexNode->getBucketOffset();
			fromHashIndex.Unpin();

			return bucketOffset;
		}


		//----------------------------------------------------------------------------------------
		//属性
		//----------------------------------------------------------------------------------------



		TblFileReader *_tblFileReader;
		DBController *_dbController;
		TestData *_testData;	//初始化测试数据输入输出对象

		int _globalDepth;
		int _maxNumPerPage;		//单页储存的索引节点最大值
		int _hashIndexPages;	//已满的索引页数
		int _numInLastPage;		//未满的索引页中索引节点数
		int _capacity;

		int _bucketNum;			//桶的总个数
		int _bufferNum;			//可用缓冲区页数的大小
		static int _mostHashBit;//高位哈希的最高参考位


	public:

		// @Parameters:	bufferNum 缓冲区页数，tblFilePath 记录的来源，dbContentFilePath 数据库中装桶的文件路径，dbIndexFilePath 数据库中装索引的文件路径
		//				TestInputFilePath 测试数据输入文件路径，TestoutputFilePath 测试结果输出路径
		// @Return:		(void)
		// @Function:	使用特定的缓冲区页数进行高位哈希，并根据测试数据进行输出
		HighHashManagement(int bufferNum, string tblFilePath, string dbContentFilePath, string dbIndexFilePath, string TestInputFilePath, string TestoutputFilePath)
		{
			initial(bufferNum, tblFilePath, dbContentFilePath, dbIndexFilePath, TestInputFilePath, TestoutputFilePath);

			Record toInsert = _tblFileReader->Read();
			while (toInsert.getOrderKey() != -1)
			{
				insert(toInsert);
				toInsert = _tblFileReader->Read();
			}

			query();
		}
		
		~HighHashManagement()
		{
			delete _tblFileReader;
			delete _dbController;
			delete _testData;
		}

		// @Parameters:	bufferNum 缓冲区页数，tblFilePath 记录的来源，dbContentFilePath 数据库中装桶的文件路径，dbIndexFilePath 数据库中装索引的文件路径
		//				TestInputFilePath 测试数据输入文件路径，TestoutputFilePath 测试结果输出路径
		// @Return:		(void)
		// @Function:	根据输入的参数初始化
		void initial(int bufferNum, string tblFilePath, string dbContentFilePath, string dbIndexFilePath, string TestInputFilePath, string TestoutputFilePath)
		{
			//初始化TblFileReader对象
			_tblFileReader = new TblFileReader(tblFilePath);
			_tblFileReader->Open();

			//初始化DBController对象
			_dbController = new DBController(bufferNum, dbContentFilePath, dbIndexFilePath);
			_dbController->Open();

			//初始化测试数据输入输出对象_testData
			_testData = new TestData(TestInputFilePath, TestoutputFilePath);

			//初始化第一个桶
			Bucket firstBucket(*_dbController);
			firstBucket.Pin();
			firstBucket.initialBucket(0, 1);
			firstBucket.Save();
			firstBucket.Unpin();

			//初始化第二个桶
			Bucket secondBucket(*_dbController);
			secondBucket.Pin();
			secondBucket.initialBucket(1, 1);
			secondBucket.Save();
			secondBucket.Unpin();
			_bucketNum = 2;

			//初始化两个哈希节点，因为全局深度为1
			HashIndex firstHashIndex = HashIndex(*_dbController);
			firstHashIndex.Pin();
			indexNode temp = indexNode(0, 0);
			firstHashIndex.insertIndex(temp);
			temp = indexNode(1,1);
			firstHashIndex.insertIndex(temp);

			//设置初始属性
			_capacity = firstHashIndex.getCapacity();
			_maxNumPerPage = firstHashIndex.getMaxNum();
			_globalDepth = 1;
			_hashIndexPages = 0;
			_numInLastPage = 2;

			//初始化可用缓冲区页数
			_bufferNum = bufferNum;

			firstHashIndex.Save();
			firstHashIndex.Unpin();
		}

		// @Parameters:	key 需要哈希的键值，depth 深度
		// @Return:		哈希后的值
		// @Function:	高位哈希函数
		static int hashFunc(int key, int depth){
			int *ptr = new int[_mostHashBit];
			for(int i=0; i<_mostHashBit; i++)
			{
				ptr[i] = key % 2;
				key /= 2;
			}

			int temp = 0;
			int scale = 1;
			for(int i=depth; i>0; i--)
			{
				temp += ptr[_mostHashBit - i] * scale;
				scale *= 2;
			}
			
			delete ptr;
			return temp;
		}

		// @Parameters:	record 待插入的记录
		// @Return:		(viod)
		// @Function:	插入记录到哈希表中
		void insert(Record record)
		{
			int index = hashFunc(record.getOrderKey(), _globalDepth);			//获得其中一个指向对应桶的哈希值
			int bucketOffset = getCorrespondBucket(index);						//获得对应桶的偏移地址

			Bucket toInsertBucket(*_dbController, bucketOffset);				//找出待插入的桶
			toInsertBucket.Pin();

			while(toInsertBucket.insert(record) == NEED_SPLIT)
			{
				if(_globalDepth == toInsertBucket.getLocalDepth())				//观察是否需要哈希索引翻倍
				{
					hashIndexSplit();
				}

				Bucket splitBucket = Bucket(*_dbController);					//将分裂桶初始化
				splitBucket.Pin();

				int localDepth = toInsertBucket.getLocalDepth();
				index = hashFunc(record.getOrderKey(), localDepth);	
				int splitIndex = (index*2 + 1) * (int)pow(2.0, _globalDepth - localDepth - 1);		//指向分裂桶的第一个指针
				splitBucket.initialBucket(-1, localDepth);											//分裂桶的_bucketID会在后面重新赋值，所以此处赋值为-1
	
				reAssignIndex(splitIndex, localDepth);							//修改分裂桶对应的索引节点指针
				reAssign(toInsertBucket, splitBucket);							//重新分配原桶和分裂桶的节点，并修改局部深度

				if ((index*2) != hashFunc(record.getOrderKey(), toInsertBucket.getLocalDepth()))
				{
					splitBucket.Save();
					toInsertBucket.Save();

					toInsertBucket.Unpin();										//将原来pin上的那一块缓冲区解pin
					toInsertBucket = splitBucket;	
					toInsertBucket.Pin();										//对新的缓冲区pin
				}
	
				_bucketNum++;
				splitBucket.Save();
				splitBucket.Unpin();	
			}

			toInsertBucket.Save();
			toInsertBucket.Unpin();
		}


		// @Parameters:	(viod)
		// @Return:		(viod)
		// @Function:	根据测试输入进行数据库查询并输出到相应文件
		void query()
		{
			int time = _testData->getTestCaseNum();

			for (int i=0; i<time; i++)
			{
				int testOrderKey = _testData->readTestIn();
				int hashNum = hashFunc(testOrderKey, _globalDepth);
				int toOpen = hashNum/_maxNumPerPage;
				HashIndex toGetIndex(*_dbController, toOpen);
				indexNode *tempNode =  toGetIndex.getIndexNode(hashNum % _maxNumPerPage + 1);
				Bucket toGetBucket(*_dbController, tempNode->getBucketOffset());


				Record recordArray[10];
				int recordNum = 0;
				for(int i=0; i<toGetBucket.getslotNumber(); i++)	//获取满足指定键值的所有记录
				{
					Record temp = toGetBucket.getRecord(i+1);
					if(temp.getOrderKey() == testOrderKey)
					{
						recordArray[recordNum] = temp;
						recordNum++;
					}
				}

				for(int i=0; i<recordNum; i++)						//将记录根据partKey升序输出
				{
					int min = INF;
					int index = -1;
					for(int j=0; j<recordNum; j++)
					{
						if(recordArray[j].getOrderKey() != -1 && recordArray[j].getPartKey() < min)
						{
							min = recordArray[j].getPartKey();
							index = j;
						}
					}

					_testData->write(recordArray[index]);
					recordArray[index].setOrderKey(-1);
				}

				_testData->writeGap();
			}
		}
	};

	int HighHashManagement::_mostHashBit = 21;
};
#endif




	

