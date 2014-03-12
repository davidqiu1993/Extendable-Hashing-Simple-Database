
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
		// @Function:	��ϣ�������ѣ�����ԭͰָ�븴�Ƹ�����Ͱ��ͬʱȫ����ȼ�һ
		void hashIndexSplit()
		{
			//��λ�й�ϣ�����ȷ��ѣ�ԭ��һ���ڵ���ѳ����������Ľڵ㣬���Ѻ�������ڵ�Ķ�ӦͰ��ԭ�ڵ���ͬ


			//��ȷ����Ҫ���ƵĹ�ϣ�����ڵ�������յ�
			HashIndex fromHashIndex = HashIndex(*_dbController, 0);
			fromHashIndex.Pin();
			int initialHashIndexPages = _hashIndexPages;
			int initialNumInLastPage = _numInLastPage;
			fromHashIndex.Save();
			fromHashIndex.Unpin();

			//�����¹�ϣ������ÿ��������Ͱƫ��ֵΪ-1
			HashIndex toHashIndex = HashIndex(*_dbController, _hashIndexPages);
			toHashIndex.Pin();

			int NodeID = (int)pow(2.0, _globalDepth);
			int insertNum = (int)pow(2.0, _globalDepth);

			for(int i=0; i<insertNum; i++)
			{
				//��ý�Ҫ����Ľڵ�
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


			//�޸Ĺ�ϣ�����ڵ��Ͱƫ��ֵ����
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
				//��ǰҳʣ��ڵ���Ϊ��
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

		// @Parameters:	index ����Ͱ�ĳ�ʼ��ַ��localDepth ԭͰ�ľֲ����
		// @Return:		(void)
		// @Function:	������Ӧ��ָ�����Ͱ�������ڵ����¶���
		void reAssignIndex(int index, int localDepth)
		{
			int splitIndex = index;
			int reAssignNum = (int)pow(2.0, _globalDepth - localDepth - 1);
			for(int i=0; i<reAssignNum; i++)
			{
				//�޸ķ���Ͱ��Ӧ�������ڵ�ָ��
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
		// @Function:	��Ͱ����ʱ���·���Ͱ�еļ�¼,��ˢ�¾ֲ����
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

		// @Parameters:	key ��Ҫ����ļ�¼�ļ�ֵ
		// @Return:		���ظü�ֵ��ϣ��ĵ�һ��Ͱƫ����
		// @Function:	���ݴ���ļ�ֵ���ض�ӦͰ��ƫ����
		int getCorrespondBucket(int key)
		{
			//�ҳ���Ӧ��ϣ�����ڵ�����ҳ
			int toOpen = key / _maxNumPerPage;
			HashIndex fromHashIndex = HashIndex(*_dbController, toOpen);
			fromHashIndex.Pin();

			//�ҳ���ָ���Ͱ��ƫ��ֵ
			indexNode *hashIndexNode = fromHashIndex.getIndexNode(key % _maxNumPerPage + 1);	//������һ��ʼ�����Դ���ʱҪ��һ
			int bucketOffset = hashIndexNode->getBucketOffset();
			fromHashIndex.Unpin();

			return bucketOffset;
		}


		//----------------------------------------------------------------------------------------
		//����
		//----------------------------------------------------------------------------------------



		TblFileReader *_tblFileReader;
		DBController *_dbController;
		TestData *_testData;	//��ʼ���������������������

		int _globalDepth;
		int _maxNumPerPage;		//��ҳ����������ڵ����ֵ
		int _hashIndexPages;	//����������ҳ��
		int _numInLastPage;		//δ��������ҳ�������ڵ���
		int _capacity;

		int _bucketNum;			//Ͱ���ܸ���
		int _bufferNum;			//���û�����ҳ���Ĵ�С
		static int _mostHashBit;//��λ��ϣ����߲ο�λ


	public:

		// @Parameters:	bufferNum ������ҳ����tblFilePath ��¼����Դ��dbContentFilePath ���ݿ���װͰ���ļ�·����dbIndexFilePath ���ݿ���װ�������ļ�·��
		//				TestInputFilePath �������������ļ�·����TestoutputFilePath ���Խ�����·��
		// @Return:		(void)
		// @Function:	ʹ���ض��Ļ�����ҳ�����и�λ��ϣ�������ݲ������ݽ������
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

		// @Parameters:	bufferNum ������ҳ����tblFilePath ��¼����Դ��dbContentFilePath ���ݿ���װͰ���ļ�·����dbIndexFilePath ���ݿ���װ�������ļ�·��
		//				TestInputFilePath �������������ļ�·����TestoutputFilePath ���Խ�����·��
		// @Return:		(void)
		// @Function:	��������Ĳ�����ʼ��
		void initial(int bufferNum, string tblFilePath, string dbContentFilePath, string dbIndexFilePath, string TestInputFilePath, string TestoutputFilePath)
		{
			//��ʼ��TblFileReader����
			_tblFileReader = new TblFileReader(tblFilePath);
			_tblFileReader->Open();

			//��ʼ��DBController����
			_dbController = new DBController(bufferNum, dbContentFilePath, dbIndexFilePath);
			_dbController->Open();

			//��ʼ���������������������_testData
			_testData = new TestData(TestInputFilePath, TestoutputFilePath);

			//��ʼ����һ��Ͱ
			Bucket firstBucket(*_dbController);
			firstBucket.Pin();
			firstBucket.initialBucket(0, 1);
			firstBucket.Save();
			firstBucket.Unpin();

			//��ʼ���ڶ���Ͱ
			Bucket secondBucket(*_dbController);
			secondBucket.Pin();
			secondBucket.initialBucket(1, 1);
			secondBucket.Save();
			secondBucket.Unpin();
			_bucketNum = 2;

			//��ʼ��������ϣ�ڵ㣬��Ϊȫ�����Ϊ1
			HashIndex firstHashIndex = HashIndex(*_dbController);
			firstHashIndex.Pin();
			indexNode temp = indexNode(0, 0);
			firstHashIndex.insertIndex(temp);
			temp = indexNode(1,1);
			firstHashIndex.insertIndex(temp);

			//���ó�ʼ����
			_capacity = firstHashIndex.getCapacity();
			_maxNumPerPage = firstHashIndex.getMaxNum();
			_globalDepth = 1;
			_hashIndexPages = 0;
			_numInLastPage = 2;

			//��ʼ�����û�����ҳ��
			_bufferNum = bufferNum;

			firstHashIndex.Save();
			firstHashIndex.Unpin();
		}

		// @Parameters:	key ��Ҫ��ϣ�ļ�ֵ��depth ���
		// @Return:		��ϣ���ֵ
		// @Function:	��λ��ϣ����
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

		// @Parameters:	record ������ļ�¼
		// @Return:		(viod)
		// @Function:	�����¼����ϣ����
		void insert(Record record)
		{
			int index = hashFunc(record.getOrderKey(), _globalDepth);			//�������һ��ָ���ӦͰ�Ĺ�ϣֵ
			int bucketOffset = getCorrespondBucket(index);						//��ö�ӦͰ��ƫ�Ƶ�ַ

			Bucket toInsertBucket(*_dbController, bucketOffset);				//�ҳ��������Ͱ
			toInsertBucket.Pin();

			while(toInsertBucket.insert(record) == NEED_SPLIT)
			{
				if(_globalDepth == toInsertBucket.getLocalDepth())				//�۲��Ƿ���Ҫ��ϣ��������
				{
					hashIndexSplit();
				}

				Bucket splitBucket = Bucket(*_dbController);					//������Ͱ��ʼ��
				splitBucket.Pin();

				int localDepth = toInsertBucket.getLocalDepth();
				index = hashFunc(record.getOrderKey(), localDepth);	
				int splitIndex = (index*2 + 1) * (int)pow(2.0, _globalDepth - localDepth - 1);		//ָ�����Ͱ�ĵ�һ��ָ��
				splitBucket.initialBucket(-1, localDepth);											//����Ͱ��_bucketID���ں������¸�ֵ�����Դ˴���ֵΪ-1
	
				reAssignIndex(splitIndex, localDepth);							//�޸ķ���Ͱ��Ӧ�������ڵ�ָ��
				reAssign(toInsertBucket, splitBucket);							//���·���ԭͰ�ͷ���Ͱ�Ľڵ㣬���޸ľֲ����

				if ((index*2) != hashFunc(record.getOrderKey(), toInsertBucket.getLocalDepth()))
				{
					splitBucket.Save();
					toInsertBucket.Save();

					toInsertBucket.Unpin();										//��ԭ��pin�ϵ���һ�黺������pin
					toInsertBucket = splitBucket;	
					toInsertBucket.Pin();										//���µĻ�����pin
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
		// @Function:	���ݲ�������������ݿ��ѯ���������Ӧ�ļ�
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
				for(int i=0; i<toGetBucket.getslotNumber(); i++)	//��ȡ����ָ����ֵ�����м�¼
				{
					Record temp = toGetBucket.getRecord(i+1);
					if(temp.getOrderKey() == testOrderKey)
					{
						recordArray[recordNum] = temp;
						recordNum++;
					}
				}

				for(int i=0; i<recordNum; i++)						//����¼����partKey�������
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




	

