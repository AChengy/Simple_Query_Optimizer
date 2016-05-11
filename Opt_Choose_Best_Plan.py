import math

class Opt_Choose_Best_Plan:

    def __init__(self, steps):
        self.executionSteps = steps
        self.QueryPlan = QueryExecution()
        self.tables = [table('t1',1000,204, 20), table('t2', 500,102,20), table('t3',2000,40,100)]
        self.correlated = False

        #go through the steps
        for step in steps:
            for i, part in enumerate(step):
                if part == "Correlated":
                    self.correlated= True
                if part == "Join":
                    if self.correlated:
                        self.QueryPlan.addCost(self.QueryPlan.totalCost * self.TNJ(step[i+1], step[i+2], step[i+3],step[i+4]))


                    else:
                        AllJoinOptions = self.testAllJoins( step[i+1] , step[i+2])
                        minVal = min(AllJoinOptions, key=AllJoinOptions.get)
                        self.QueryPlan.addExecutionStep(minVal)
                        self.QueryPlan.addCost(AllJoinOptions[minVal])
                        #print "Query Plan: "  , self.QueryPlan.totalCost

                        #Setup the table with the new Values and add to the list of Tables
                        selectivity = float(step[len(step)-2])
                        # print "Selectivity: ", selectivity

                        NewTableName = step[len(step)-1]
                       # print "New Table Name: ", NewTableName

                        NewTableSize = self.getTableSize(step[1]) * self.getTableSize(step[2]) * selectivity
                        #print "New Table Size: ", NewTableSize

                        self.tables.append(table(NewTableName, NewTableSize, None, None))
                        #set table to sorted
                        if minVal == "SMJL" or minVal == "SMJM":
                            self.changeSorted(NewTableName)
                        # print "Test All Joins: ", AllJoinOptions
                        # print "MinVal: ", minVal
                elif part == "project":
                    self.QueryPlan.addCost(self.projection(step[i+1], step[i+2]))
                    #print "Query Plan: "  , self.QueryPlan.totalCost
                elif part == "GroupBy" or part == "Aggregate":
                    self.QueryPlan.addCost(self.Grouper(step))
                    # print "Query Plan: " , self.QueryPlan.totalCost

    def changeSorted(self, tableToChange):
        for table in self.tables:
            if table.name == tableToChange:
                table.sorted == True

    def getSorted(self, tableToCheck):
        for table in self.tables:
            if table.name == tableToCheck:
                return table.sorted


    def Grouper(self, listSteps):
        #print " Grouper - list Steps: ", listSteps

        if 'without' in listSteps:
            # print "I AM HERE. "
            # print "List Steps 1: ", listSteps[1]
            # print "Last table: ", self.tables[len(self.tables)-1].name
            return self.getTableSize(self.tables[len(self.tables)-1].name)
        else:
            #print "List Steps 1: ", listSteps[1]
            size = self.getTableSize(listSteps[1])
            self.tables.append(table(listSteps[len(listSteps)-1], size * float(listSteps[len(listSteps)-2]), None, None))
            if not listSteps[3] == "None":
                return 2*(self.getTableSize(listSteps[1])+self.getTableSize(listSteps[3]))  #for scan and a sort
            else:
                return 2*(self.getTableSize(listSteps[1]))



    def getTableSize(self,tableName):
        #for table in self.tables:
            #print "tableName", table.name, " Table Size: ", table.tableSize
        for table in self.tables:
            if table.name == tableName:
                return table.tableSize

    def projection(self,CurrTable, newTable):
       # print "Curr Table: ", CurrTable

        tableSize = float(self.getTableSize(CurrTable))
       # print "newtable: ", newTable
        newTableSize = tableSize
        #print "New Table Size: ", newTableSize
        tempTable = table(newTable, newTableSize, None, None)
        self.tables.append(tempTable)
        if not self.getSorted(CurrTable):
            #print "tableSize: ", tableSize
            return 3*tableSize + math.log10(tableSize)
        else:
            return 2*tableSize




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

    #Method for finding the TNJ costs
    def TNJ(self, left, right, selectivity, newTableName):
        for table1 in self.tables:
            if left == table1.name:
                left = table1
            if right == table1.name:
                right = table1

        #create the new table


        newTupleSize = left.tupleSize + right.tupleSize
        #print "newTupleSize:  ", newTupleSize
        NewTableSize = float(selectivity) * left.tableSize * right.tableSize * (newTupleSize/4096.0)
        #print "New Table Size: ", NewTableSize
        #print "Left Table size: ", left.tableSize, " Right Table Size: ", right.tableSize
        self.tables.append(table(newTableName, NewTableSize, (4096/newTupleSize), newTupleSize))


        return left.tableSize + (left.numTuples*left.tableSize)*right.tableSize

    #method for finding PNL Costs
    def PNL(self, temp1, temp2):
        #print "temp1 name: ", temp1
        #get left Cost
        costLeft = self.getTableSize(temp1)

        #print "temp2 name: ", temp2
        #get right Cost
        costRight = self.getTableSize(temp2)

        #print "Cost Left: ", costLeft
        #print "Cost Right: ", costRight
        return min((costLeft + costLeft*costRight), (costRight + costRight*costLeft))

    #Method for finding BNJM costs
    def BNJM(self, temp1, temp2):
        #print "temp1 name: ", temp1.name

        buffer = 50

        #print "temp1 name: ", temp1
        #get left Cost
        costLeft = self.getTableSize(temp1)

        #print "temp2 name: ", temp2
        #get right Cost
        costRight = self.getTableSize(temp2)

        #print "Cost Left: ", costLeft
        #print "Cost Right: ", costRight
        return min((costLeft + (costLeft/buffer)*costRight), (costRight + (costRight/buffer)*costLeft))

    def SMJM(self, temp1, temp2):
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
        return 2*3*(costLeft + costRight)

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
    def __init__(self,name, size, numTuples, tupeSize):
        self.pageSize = 4096
        self.blockSize = 100
        # self.t1Size = 1000
        # self.t2Size = 500
        # self.t3Size = 2000
        self.tableSize = size
        self.name = name
        self.sorted = False
        self.numTuples = numTuples
        self.tupleSize = tupeSize


class QueryExecution:
    def __init__(self):
        self.totalCost = 1
        self.steps = []

    def addCost(self, cost):
        self.totalCost += cost

    def addExecutionStep(self, step):
        self.steps.append(step)
