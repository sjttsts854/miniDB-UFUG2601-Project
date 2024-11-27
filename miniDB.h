#ifndef MINIDB_H
#define MINIDB_H

#include "Table.h"
#include <map>
#include <string>
#include <vector>

// 使用命名空间 std
using std::map;
using std::string;
using std::vector;

// miniDB 类表示一个简单的数据库管理系统
class miniDB 
{
public:
    map<string, Table> tables; // 存储所有表的映射
    string currentDB; // 当前数据库

    // 构造函数
    miniDB()=default;

    // 创建数据库
    void CreateDataBase(const string& dbname);

    // 使用数据库
    void UseDataBase(const string& dbname);

    // 创建表
    void CreateTable(const string& tableName, const vector<string>& columns, const vector<string>& columnTypes);

    // 删除表
    void DropTable(const string& tableName);

    // 向表中插入数据
    void InsertIntoTable(const string& tableName, const vector<string>& row);

    // 从表中选择特定列的数据
    void SelectColumnsFromTable(const string& tableName, const vector<string>& columns, const string& outputFile);

    // 从表中选择所有列的数据
    void SelectAllColumnsFromTable(const string& tableName, const string& outputFile);

    // 从表中选择带有条件的数据
    void SelectFromTableWithConditions(const string& tableName, const vector<string>& columns, const string& conditions, const string& outputFile);

    // 更新表中的数据
    //void UpdateTable(const string& tableName, const vector<string>& setClauses, const string& conditions);


    // 从文件加载表
    void LoadAllTables(const string& path);
};

// 解析并执行命令
void parseCommand(const string& command, miniDB& db, const string& outputFile);

#endif // MINIDB_H