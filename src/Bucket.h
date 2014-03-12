#ifndef _BUCKET_H_
#define _BUCKET_H_

#include "General.h"

namespace exth
{
	//ָʾ���뵽��ǰ������ҳ�ķ���״̬
	enum InsertState 
	{
		SUCCEED = 0,
		NEED_SPLIT = 1
	};

	//Ͱ�ڲ۶���
	class Slot
	{
	private:
		int _offset;	//�ò���ָ���¼�Ĵ�����ʼƫ����
		int _rID;		//��¼��orderKeyֵ
		int _rLen;		//��¼��������ʽ����

	public:
		Slot()
		{
			_offset = -1;
			_rID = 0;
			_rLen = 0;
		}
		Slot(int offset, int rID, int rLen)
		{
			_offset = offset;
			_rID = rID;
			_rLen = rLen;
		}
		
		int getOffset()
		{
			return _offset;
		}
		void setOffset(int offset)
		{
			_offset = offset;
		}

		int getRID()
		{
			return _rID;
		}
		int getRLen()
		{
			return _rLen;
		}
	};

	class Bucket
	{
	private:

		// @Parameters:	toSlotPtr ָ����ʼָ���ָ�룬��ΪҪ�޸���ʼָ�룬���Դ���ָ���ָ�룻endPtr ��ʾ��ָֹ��
		// @Return:		(void)
		// @Function:	������ʼ����ָֹ�룬Ѱ�ҳ���һ���ղ�
		void nextToSlotPtr(char** toSlotPtr, char* endPtr)
		{
			while(*toSlotPtr != endPtr)
			{
				int offset = *((int*)(*toSlotPtr));
				if(offset != -1)
					(*toSlotPtr) += sizeof(Slot);
				else
					break;
			}
		}

		// @Parameters:	fromSlotPtr ָ����ʼָ���ָ�룬��ΪҪ�޸���ʼָ�룬���Դ���ָ���ָ�룻endPtr ��ʾ��ָֹ��
		// @Return:		(void)
		// @Function:	������ʼ����ָֹ�룬Ѱ�ҳ���һ��֮ǰ�пղ۵ķǿղ�
		void nextFromSlotPtr(char **fromSlotPtr, char *endPtr)
		{
			while(*fromSlotPtr != endPtr)
			{
				int offset = *((int*)(*fromSlotPtr));
				if(offset == -1)
					(*fromSlotPtr) += sizeof(Slot);
				else
					break;
			}
		}

		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	�����в��������У������пղ۸��ǣ��������޸�ÿ���ǿղ���ָ���¼��λ��
		void resortSlot()
		{
			char *startPtr = (char*)_bucketBuffer + *_slotStart;
			char *endPtr = (char*)_bucketBuffer + *_slotStart + (*_slotNumber) * sizeof(Slot);
			Slot temp;

			while(startPtr != endPtr)				//ͳ���ж��ٷǿղ�
			{
				temp = *((Slot*)startPtr);
				if(temp.getOffset() == -1)
				{
					(*_slotNumber)--;
					*_freeStart -= sizeof(Slot);
					*_freeLen += sizeof(Slot);
				}

				startPtr += sizeof(Slot);
			}

			char *toSlotPtr = (char*)_bucketBuffer + *_slotStart;
			nextToSlotPtr(&toSlotPtr, endPtr);

			char *fromSlotPtr = toSlotPtr;
			nextFromSlotPtr(&fromSlotPtr, endPtr);

			while(fromSlotPtr != endPtr)			//����ղ�
			{
				memcpy(toSlotPtr, fromSlotPtr, sizeof(Slot));
				*((int*)fromSlotPtr) = -1;

				nextToSlotPtr(&toSlotPtr, endPtr);
				nextFromSlotPtr(&fromSlotPtr, endPtr);
			}
		}

		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	�����������еĲۣ������Ӧ�ļ�¼�������У�����¼��Ŀ�λ����
		void resortRecord()
		{
			char *endSlotPtr = (char*)_bucketBuffer + *_slotStart + sizeof(Slot)*(*_slotNumber);
			char *insertPtr = (char*)_bucketBuffer + _capacity;
			int currentOffset = _capacity;

			for(char* iptr = (char*)_bucketBuffer + *_slotStart;
				iptr!=endSlotPtr; iptr+=sizeof(Slot))
			{
				Slot temp = *((Slot*)iptr);
				currentOffset -= temp.getRLen();
				insertPtr -= temp.getRLen();

				memmove(insertPtr, (char*)_bucketBuffer + temp.getOffset(), temp.getRLen());
				((Slot*)iptr)->setOffset(currentOffset);
			}

			(*_freeLen) = currentOffset - (*_freeStart);
			Save();
		}


		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	��Bucket������ָ��Ļ�����ҳ�ж�ȡ�����Ϣ
		void readBucket()
		{
			_bucketBuffer = _bucket->getBuffer();
			_capacity = _bucket->getCapacity();

			_bucketID = (int*)_bucketBuffer;
			_bucketAddress = _bucketID + 1;
			_localDepth = _bucketAddress + 1;
			_slotStart = _localDepth + 1;
			_slotNumber = _slotStart + 1;
			_freeStart = _slotNumber + 1;
			_freeLen = _freeStart + 1;
		}

		void setBucketAddress(const int &bucketAddress)
		{
			*_bucketAddress = bucketAddress;
		}



//----------------------------------------------------------------------
//����
//----------------------------------------------------------------------



		//����洢��Ͱ��
		MemoryBuffer *_bucket;
		void *_bucketBuffer;	//���ڼ�¼Ͱ���ڴ��еĶ�Ӧ��������ʼ��ַ
		int _capacity;			//ÿ��Ͱ����(�ֽ�Ϊ��λ)


		//ָ����ָ������԰�˳��洢��ÿ��Ͱ��
		int *_bucketID;		//��Ͱ�Ĺ�ϣ����ֵ(����ھֲ����)
		int *_bucketAddress;//��Ͱ���ļ��д����ƫ��ֵ��һ��ҳƫ��ֵΪ1��ƫ������0��ʼ
		int *_localDepth;
		int *_slotStart;	//��¼������ʼλ�õ��ֽ�ƫ����
		int *_slotNumber;	//���в۵��ܸ����������ղ�
		int *_freeStart;	//��¼���пռ���ʼλ�õ��ֽ�ƫ����
		int *_freeLen;		//���пռ����ֽ���

		

	public:
		// @Parameters:	dbController �����DBControllerʵ�������ڶ����ݿ����ݽ��в���
		// @Return:		(void)
		// @Function:	ͨ�������DBControllerʵ���������µ�bucket����
		Bucket(DBController &dbController){
			setBucket(dbController);
		}

		// @Parameters:	dbController �����DBControllerʵ�������ڶ����ݿ����ݽ��в�����pageNumber Ҫ��ȡ��Ͱ�����ݿ��е�ҳƫ����
		// @Return:		(void)
		// @Function:	���ݴ����ҳƫ����(��0��ʼ)��ͨ�������DBControllerʵ������ȡ��Ӧ��Ͱҳ��������ӦBucket����
		Bucket(DBController &dbController, int pageNumber){
			setBucket(dbController, pageNumber);
		}

		// @Parameters: origin ��Ҫ���Ƶ�ʵ��
		// @Return:		(void)
		// @Function:	���ݴ����ʵ������һ���µ�Bucket����
		Bucket(const Bucket& origin)
		{
			this->_bucket = origin._bucket;
			this->_bucketBuffer = origin._bucketBuffer;
			this->_capacity = origin._capacity;

			this->_bucketID = origin._bucketID;
			this->_bucketAddress = origin._bucketAddress;
			this->_localDepth = origin._localDepth;
			this->_slotStart = origin._slotStart;
			this->_slotNumber = origin._slotNumber;
			this->_freeStart = origin._freeStart;
			this->_freeLen = origin._freeLen;
		}

		// @Parameters: origin ��ֵ��������Դ
		// @Return:		��ֵ���������
		// @Function:	��ֵ��������
		Bucket& operator =(const Bucket& origin)
		{
			this->_bucket = origin._bucket;
			this->_bucketBuffer = origin._bucketBuffer;
			this->_capacity = origin._capacity;

			this->_bucketID = origin._bucketID;
			this->_bucketAddress = origin._bucketAddress;
			this->_localDepth = origin._localDepth;
			this->_slotStart = origin._slotStart;
			this->_slotNumber = origin._slotNumber;
			this->_freeStart = origin._freeStart;
			this->_freeLen = origin._freeLen;

			return (*this);
		}



		// @Parameters:	dbController �����DBController�������ڶ�ȡҳ��pageNumber Ҫ��ȡ��ҳƫ����
		// @Return:		(void)
		// @Function:	����ҳƫ������ȡ��Ӧ��Ͱ���������У��Ὣԭ��Bucket�����е���Ϣ�����Ҳ����Զ�����ԭ����Ϣ
		void setBucket(DBController &dbController)
		{
			int pageNumber = dbController.AppendNewPage(DatabaseContentFile);
			_bucket = &dbController.GetBufferMappingToPage(DatabaseContentFile, pageNumber);
			readBucket();
		}

		// @Parameters:	dbController �����DBController�������ڶ�ȡҳ��pageNumber Ҫ��ȡ��ҳƫ����
		// @Return:		(void)
		// @Function:	����ҳƫ������ȡ��Ӧ��Ͱ���������У��Ὣԭ��Bucket�����е���Ϣ�����Ҳ����Զ�����ԭ����Ϣ
		void setBucket(DBController &dbController, int pageNumber)
		{
			_bucket = &dbController.GetBufferMappingToPage(DatabaseContentFile, pageNumber);
			readBucket();
		}

		// @Parameters:	bucketID Ͱ��ţ�ָ����Ͱ�Ĺ�ϣֵ��localDepth Ͱ�ľֲ����
		// @Return:		(void)
		// @Function:	���ݴ���Ĳ���Ϊ�����Ը�ֵ
		void initialBucket(int bucketID ,int localDepth)
		{
			int offset = sizeof(int)*7;

			setBucketID(bucketID);
			setBucketAddress(_bucket->getPageNumber());
			setlocalDepth(localDepth);
			setslotStart(offset);
			setslotNumber(0);
			setFreeStart(offset);
			setFreeLen(_capacity - offset);
		}

		// @Parameters:	toInsert ����������¼
		// @Return:		���ز���״̬��Ϣ���ֱ�Ϊ��Ҫ���ѺͲ���ɹ��������
		// @Function:	����һ����¼����ǰҳ����������Ӧ�ĳɹ���ʧ����Ϣ
		InsertState insert(Record toInsert)
		{
			int length = 0;		//������ʾ��¼����
			void *recordPtr = toInsert.GetBytes(length);

			if ((length + sizeof(Slot)) > (*_freeLen))
				return NEED_SPLIT;
			else
			{
				//�����¼
				(*_freeLen) -= length;
				char *insertPtr = (char*)_bucketBuffer + (*_freeStart) + (*_freeLen);	
				memcpy(insertPtr, recordPtr, length);

				//�����Ӧ��
				char *slotPtr = (char*)_bucketBuffer + (*_slotStart) + (*_slotNumber) * sizeof(Slot);
				Slot newSlot = Slot((*_freeStart) + (*_freeLen), toInsert.getOrderKey(), length);
				memcpy(slotPtr, &newSlot, sizeof(Slot));

				(*_slotNumber)++;
				(*_freeStart) += sizeof(Slot);
				(*_freeLen) -= sizeof(Slot);

				return SUCCEED;
			}	
		}

		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	������֯Ͱ�ڵ����ݣ������Լ���¼�ڲ��Ŀ��пռ�ȫ������
		void resort()
		{
			resortSlot();
			resortRecord();
		}

		// @Parameters:	index ��ʾҪ�Ƴ��ڼ�����¼,��1��ʼ
		// @Return:		����Ҫ�Ƴ��ļ�¼������Ƴ����ɹ��򷵻�orderkeyΪ-1�ļ�¼
		// @Function:	������ʾ�Ƴ���ؼ�¼����������Ӧ�ĳɹ���ʧ����Ϣ
		Record remove(int index)
		{
			Record toReturn = Record();

			if(index<=0 || index>(*_slotNumber))		//ƫ����Խ��
			{
				toReturn.setOrderKey(-1);
				return toReturn;
			}
			else
			{
				Slot temp = Slot();
				char *slotPtr = (char*)_bucketBuffer + *_slotStart + (index-1)*sizeof(Slot);
				memcpy(&temp, slotPtr, sizeof(Slot));

				if(temp.getOffset() == -1)			//�ò���Ч
				{
					toReturn.setOrderKey(-1);
					return toReturn;
				}
				else								//�����Ϸ�
				{
					char *recordPtr = (char*)_bucketBuffer + temp.getOffset();
					toReturn = Record::Parse((void*)recordPtr, temp.getRLen());
					*((int*)slotPtr) = -1;
					return toReturn;
				}
			}
		}

		// @Parameters:	index ��ʾҪ���صڼ�����¼,��1��ʼ
		// @Return:		���ؼ�¼�����û����ؼ�¼�򷵻�orderkeyΪ-1�ļ�¼
		// @Function:	������ʾ������ؼ�¼����������Ӧ�ĳɹ���ʧ����Ϣ
		Record getRecord(int index)
		{
			Record toReturn = Record();

			if(index<=0 || index>(*_slotNumber))		//ƫ����Խ��
			{
				toReturn.setOrderKey(-1);
				return toReturn;
			}
			else
			{
				Slot temp = Slot();
				char *slotPtr = (char*)_bucketBuffer + *_slotStart + (index-1)*sizeof(Slot);
				memcpy(&temp, slotPtr, sizeof(Slot));

				if(temp.getOffset() == -1)			//�ò���Ч
				{
					toReturn.setOrderKey(-1);
					return toReturn;
				}
				else								//�����Ϸ�
				{
					char *recordPtr = (char*)_bucketBuffer + temp.getOffset();
					return Record::Parse((void*)recordPtr, temp.getRLen());
				}
			}
		}

		// @Parameters:	(void)
		// @Return:		A bool indicating whether the buffer is saved to the corresponding disk page successfully
		// @Function:	Save the buffer to the corresponding disk page indicated by the page number.
		void Save()
		{
			_bucket->Save();
		}

		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	Pin one pin on this memory buffer.
		void Pin()
		{
			_bucket->Pin();
		}

		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	Unpin one pin off this memory buffer.
		void Unpin()
		{
			_bucket->Unpin();
		}


		// @Parameters:	(void)
		// @Return:		(void)
		// @Function:	���ظ�Bucket����������Ļ�������ʼָ��
		void* getBucketBufferPtr()
		{
			return _bucketBuffer;
		}

		const int& getBucketID()
		{
			return *_bucketID;
		}
		void setBucketID(const int &bucketID)
		{
			*_bucketID = bucketID;
		}

		const int& getBucketAddress()
		{
			/*			return *_bucketAddress;*/
			return _bucket->getPageNumber();
		}

		const int& getLocalDepth()
		{
			return *_localDepth;
		}
		void setlocalDepth(const int &localDepth)
		{
			*_localDepth = localDepth;
		}

		const int& getslotStart()
		{
			return *_slotStart;
		}
		void setslotStart(const int &slotStart)
		{
			*_slotStart = slotStart;
		}

		const int& getslotNumber()
		{
			return *_slotNumber;
		}
		void setslotNumber(const int &slotNumber)
		{
			*_slotNumber = slotNumber;
		}

		const int& getFreeStart()
		{
			return *_freeStart;
		}
		void setFreeStart(const int &freeStart)
		{
			*_freeStart = freeStart;
		}

		const int& getFreeLen()
		{
			return *_freeLen;
		}
		void setFreeLen(const int &freeLen)
		{
			*_freeLen = freeLen;
		}

		const int& getCapacity()
		{
			return _capacity;
		}
		void setCapacity(const int &capacity)
		{
			_capacity = capacity;
		}
	};
};
#endif	//_BUCKET_H_
