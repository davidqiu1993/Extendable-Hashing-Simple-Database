#ifndef _TEST_DATA_H_
#define _TEST_DATA_H_

#include "General.h"

namespace exth
{
	class TestData
	{
	private:
		string _TestInputFilePath;		//�������������ļ�·��
		FILE* _TestInputFilePtr;

		string _TestoutputFilePath;		//���Խ������ļ�·��
		FILE* _TestoutputFilePtr;

		int _testCaseNum;				//���԰�������

	public:

		// @Parameters: TestInputFilePath �������������ļ�·����TestoutputFilePath ���Խ�����·��
		// @Return:		(void)
		// @Function:	���ݲ��������������·����ʼ��������Ӧ�ļ�����ȡ���԰�������
		TestData(string TestInputFilePath, string TestoutputFilePath)
		{
			this->_TestInputFilePath = TestInputFilePath;
			this->_TestoutputFilePath = TestoutputFilePath;
			this->_TestInputFilePtr = fopen(_TestInputFilePath.c_str(), "r");
			this->_TestoutputFilePtr = fopen(_TestoutputFilePath.c_str(), "a+");
			
			fscanf(_TestInputFilePtr, "%d\n", &_testCaseNum);
		}

		~TestData()
		{
			fclose(_TestInputFilePtr);
			fclose(_TestoutputFilePtr);
		}

		// @Parameters: (void)
		// @Return:		�Ӳ������������ļ��ж�ȡ��һ����������
		// @Function:	�Ӳ������������ļ��ж�ȡһ���������ݣ������ļ�ָ��ָ����һ������
		int readTestIn()
		{
			int orderKey;
			fscanf(_TestInputFilePtr, "%d\n", &orderKey);
			return orderKey;
		}

		// @Parameters: toWrite Ҫ�������Ӧ�ļ��ļ�¼
		// @Return:		(void)
		// @Function:	������ļ�¼�����������������ļ���
		void write(Record toWrite)
		{
			fprintf(_TestoutputFilePtr, "%d|%d|%d|%d|%d|%.2lf|%.2lf|%.2lf|%c|%c|%d-%d-%d|%d-%d-%d|%d-%d-%d|%s|%s|%s|\n",
				toWrite.getOrderKey(), toWrite.getPartKey(), toWrite.getSuppKey(), toWrite.getLineNumber(), (int)toWrite.getQuantity(), 
				toWrite.getExtendedPrice(), toWrite.getDiscount(), toWrite.getTax(), toWrite.getReturnFlag(), toWrite.getLineStatus(),
				toWrite.getShipDate().getYear(), toWrite.getShipDate().getMonth(), toWrite.getShipDate().getDay(),
				toWrite.getCommitDate().getYear(), toWrite.getCommitDate().getMonth(), toWrite.getCommitDate().getDay(),
				toWrite.getReceiptDate().getYear(), toWrite.getReceiptDate().getMonth(), toWrite.getReceiptDate().getDay(),
				toWrite.getShipInStruct(), toWrite.getShipMode(), toWrite.getComment());
		}

		// @Parameters: (void)
		// @Return:		(void)
		// @Function:	������ÿ�����԰���֮���������ʶ������ķ���
		void writeGap()
		{
			fprintf(_TestoutputFilePtr, "-1\n");
		}

		// @Parameters: (void)
		// @Return:		���԰�������
		// @Function:	���ز��԰�������
		int getTestCaseNum()
		{
			return _testCaseNum;
		}
	};
};
#endif


