#ifndef MINIDB_H
#define MINIDB_H

#include "Table.h"
#include <map>
#include <string>

// miniDB 类表示一个简单的数据库管理系统
class miniDB 
{
public:
    std::map<std::string, Table> tables; // 存储所有表的映射

    // 构造函数
    miniDB()=default;

    // 创建数据库
    void CreateDataBase(const std::string& dbname);

    // 使用数据库
    void UseDataBase(const std::string& dbname);

    // 创建表
    void CreateTable(const std::string& tableName, const std::vector<std::string>& columns, const std::vector<std::string>& columnTypes);

    // 删除表
    void DropTable(const std::string& tableName);

    // 向表中插入数据
    void InsertIntoTable(const std::string& tableName, const std::vector<std::string>& row);

    // 从表中选择数据
    void SelectFromTable(const std::string& tableName);

    // 从表中选择特定列的数据
    void SelectColumnsFromTable(const std::string& tableName, const std::vector<std::string>& columns);

    //从表中选择所有列的数据
    void SelectAllColumnsFromTable(const std::string& tableName);

    //从表中选择带有条件的数据
    void SelectFromTableWithCondition(const std::string& tableName, const std::string& column, const std::string& condition);
    
    // 将表数据保存到文件
    void SaveTable(const std::string& tableName, const std::string& outputFile);

    // 从文件加载表数据
    void LoadTable(const std::string& tableName, const std::string& inputFile);

    // 将表数据保存为 CSV 文件
    void SaveTableToCSV(const std::string& tableName, const std::string& outputFile);

    // 将查询结果保存为 CSV 文件
    void SelectColumnsFromTableToCSV(const std::string& tableName, const std::vector<std::string>& columns, const std::string& outputFile);
};

// 解析并执行命令
void parseCommand(const std::string& command, miniDB& db, const std::string& outputFile);

#endif // MINIDB_H