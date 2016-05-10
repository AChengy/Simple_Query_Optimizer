import math

class Opt_Choose_Best_Plan:

    def __init__(self, steps):
        self.executionSteps = steps
        self.QueryPlan = QueryExecution()
        self.tables = [table('t1',1000), table('t2', 500), table('t3',2000)]

        #go through the steps
        for step in steps:
            for i, part in enumerate(step):
                if part == "Join":
                    AllJoinOptions = self.testAllJoins( step[i+1] , step[i+2])
                    minVal = min(AllJoinOptions, key=AllJoinOptions.get)
                    self.QueryPlan.addExecutionStep(minVal)
                    self.QueryPlan.addCost(AllJoinOptions[minVal])
                    print "Query Plan: "  , self.QueryPlan.totalCost

                    #Setup the table with the new Values and add to the list of Tables
                    selectivity = float(step[len(step)-2])
                    #print "Selectivity: ", selectivity

                    NewTableName = step[len(step)-1]
                    #print "New Table Name: ", NewTableName

                    NewTableSize = self.getTableSize(step[1]) * selectivity
                    #print "New Table Size: ", NewTableSize

                    self.tables.append(table(NewTableName, NewTableSize))
                    print "Test All Joins: ", AllJoinOptions
                    print "MinVal: ", minVal
                elif part == "project":
                    self.QueryPlan.addCost(self.projection(step[i+1], step[i+2]))
                    print "Query Plan: "  , self.QueryPlan.totalCost
                elif part == "GroupBy" or part == "Aggregate":
                    self.QueryPlan.addCost(self.Grouper(step))
                    print "Query Plan: " , self.QueryPlan.totalCost

    def Grouper(self, listSteps):
        print " Grouper - list Steps: ", listSteps

        if 'without' in listSteps:
            print "I AM HERE. "
            print "List Steps 1: ", listSteps[1]
            print "Last table: ", self.tables[len(self.tables)-1].name
            return self.getTableSize(self.tables[len(self.tables)-1].name)
        else:
            print "List Steps 1: ", listSteps[1]
            size = self.getTableSize(listSteps[1])
            self.tables.append(table(listSteps[len(listSteps)-1], size * float(listSteps[len(listSteps)-2])))
            return size + size * math.log(size,2)  #for scan and a sort

    def getTableSize(self,tableName):
        #for table in self.tables:
            #print "tableName", table.name, " Table Size: ", table.tableSize
        for table in self.tables:
            if table.name == tableName:
                return table.tableSize

    def projection(self,CurrTable, newTable):
        tableSize = float(self.getTableSize(CurrTable))
       # print "newtable: ", newTable
        newTableSize = tableSize * .20
        #print "New Table Size: ", newTableSize
        tempTable = table(newTable, newTableSize)
        self.tables.append(tempTable)
        return tableSize + math.log10(tableSize)




    def testAllJoins(self, table1, table2):
        Costs = {}

        Costs['PNL']  = self.PNL( table1, table2)
        Costs['BNJM'] = self.BNJM(table1, table2)
        Costs['SMJM'] = self.SMJM(table1, table2)
        Costs['HJM']  = self.HJM(table1, table2)
        Costs['HJL']  = self.HJL(table1, table2)
        Costs['BNJL'] = self.BNJL(table1, table2)
        Costs['SMJL'] = self.SMJL(table1, table2)

        return Costs

    def PNL(self, temp1, temp2):
        print "temp1 name: ", temp1
        #get left Cost
        costLeft = self.getTableSize(temp1)

        print "temp2 name: ", temp2
        #get right Cost
        costRight = self.getTableSize(temp2)

        print "Cost Left: ", costLeft
        print "Cost Right: ", costRight
        return min((costLeft + costLeft*costRight), (costRight + costRight*costLeft))

    def BNJM(self, temp1, temp2):
        #print "temp1 name: ", temp1.name

        buffer = 50

        print "temp1 name: ", temp1
        #get left Cost
        costLeft = self.getTableSize(temp1)

        print "temp2 name: ", temp2
        #get right Cost
        costRight = self.getTableSize(temp2)

        #print "Cost Left: ", costLeft
        #print "Cost Right: ", costRight
        return min((costLeft + (costLeft/buffer)*costRight), (costRight + (costRight/buffer)*costLeft))

    def SMJM(self, temp1, temp2):
        #print "temp1 name: ", temp1.name

        buffer = 50

        print "temp1 name: ", temp1
        #get left Cost
        costLeft = self.getTableSize(temp1)

        print "temp2 name: ", temp2
        #get right Cost
        costRight = self.getTableSize(temp2)

        # print "Cost Left: ", costLeft
        # print "Cost Right: ", costRight
        return 3*(costLeft + costRight)

    def HJM(self, temp1, temp2):
        #print "temp1 name: ", temp1.name

        buffer = 50

        #print "temp1 name: ", temp1
        #get left Cost
        costLeft = self.getTableSize(temp1)

        #print "temp2 name: ", temp2
        #get right Cost
        costRight = self.getTableSize(temp2)

        # print "Cost Left: ", costLeft
        # print "Cost Right: ", costRight

        #print "Cost Left: ", costLeft
        #print "Cost Right: ", costRight
        return 3*(costLeft + costRight)

    def HJL(self, temp1, temp2):
        #print "temp1 name: ", temp1.name

        buffer = 50

        #print "temp1 name: ", temp1
        #get left Cost
        costLeft = self.getTableSize(temp1)

       # print "temp2 name: ", temp2
        #get right Cost
        costRight = self.getTableSize(temp2)

        #print "Cost Left: ", costLeft
        #print "Cost Right: ", costRight
        return 3*(costLeft + costRight)

    def BNJL(self, temp1, temp2):
        #print "temp1 name: ", temp1.name

        buffer = 30
        #print "temp1 name: ", temp1
        #get left Cost
        costLeft = self.getTableSize(temp1)

        #print "temp2 name: ", temp2
        #get right Cost
        costRight = self.getTableSize(temp2)

        #print "Cost Left: ", costLeft
        #print "Cost Right: ", costRight
        return min((costLeft + (costLeft/buffer)*costRight), (costRight + (costRight/buffer)*costLeft))

    def SMJL(self, temp1, temp2):
        #print "temp1 name: ", temp1.name

        buffer = 30

        #print "temp1 name: ", temp1
        #get left Cost
        costLeft = self.getTableSize(temp1)

        #print "temp2 name: ", temp2
        #get right Cost
        costRight = self.getTableSize(temp2)

        #print "Cost Left: ", costLeft
        #print "Cost Right: ", costRight
        return 3*(costLeft + costRight)






class table:
    def __init__(self,name, size):
        self.pageSize = 4096
        self.blockSize = 100
        # self.t1Size = 1000
        # self.t2Size = 500
        # self.t3Size = 2000
        self.tableSize = size
        self.name = name

class QueryExecution:
    def __init__(self):
        self.totalCost = 0
        self.steps = []

    def addCost(self, cost):
        self.totalCost += cost

    def addExecutionStep(self, step):
        self.steps.append(step)
