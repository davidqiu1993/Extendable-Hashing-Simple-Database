#ifndef _TEST_DATA_H_
#define _TEST_DATA_H_

#include "General.h"

namespace exth
{
	class TestData
	{
	private:
		string _TestInputFilePath;		//测试数据输入文件路径
		FILE* _TestInputFilePtr;

		string _TestoutputFilePath;		//测试结果输出文件路径
		FILE* _TestoutputFilePtr;

		int _testCaseNum;				//测试案例总数

	public:

		// @Parameters: TestInputFilePath 测试数据输入文件路径，TestoutputFilePath 测试结果输出路径
		// @Return:		(void)
		// @Function:	根据测试数据输入输出路径初始化，打开相应文件并获取测试案例总数
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
		// @Return:		从测试数据输入文件中读取的一条测试数据
		// @Function:	从测试数据输入文件中读取一条测试数据，并将文件指针指向下一条数据
		int readTestIn()
		{
			int orderKey;
			fscanf(_TestInputFilePtr, "%d\n", &orderKey);
			return orderKey;
		}

		// @Parameters: toWrite 要输出到相应文件的记录
		// @Return:		(void)
		// @Function:	将传入的记录输出到测试数据输出文件中
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
		// @Function:	用于在每个测试案例之后加上用于识别结束的符号
		void writeGap()
		{
			fprintf(_TestoutputFilePtr, "-1\n");
		}

		// @Parameters: (void)
		// @Return:		测试案例总数
		// @Function:	返回测试案例总数
		int getTestCaseNum()
		{
			return _testCaseNum;
		}
	};
};
#endif


