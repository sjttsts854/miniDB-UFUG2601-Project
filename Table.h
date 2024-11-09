#ifndef TABLE_H
#define TABLE_H

#include <string>
#include <vector>

class Table
{
public:
    std::string name;
    std::vector<std::string> columns;
    std::vector<std::vector<std::string>> data;

    Table(const std::string& name, const std::vector<std::string>& columns)
        : name(name), columns(columns) {}
    void addRow(const std::vector<std::string>& row);
    void saveToFile(const std::string& outputFile) const;
    void loadFromFile(const std::string& inputFile);
};

#endif