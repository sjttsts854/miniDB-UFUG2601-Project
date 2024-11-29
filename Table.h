#ifndef TABLE_H
#define TABLE_H

#include <string>
#include <vector>

class Table
{
public:
    std::string name;
    std::vector<std::string> columns;
    std::vector<std::string> columnTypes;
    std::vector<std::vector<std::string>> data;
    Table()=default;
    Table(const std::string& name, const std::vector<std::string>& columns, const std::vector<std::string>& columnTypes);
    void addRow(const std::vector<std::string>& row);
    void saveToCSV(const std::string& outputFile) const;
};

#endif