##################################################
###         Alex Chengelis - 2632220           ###
###           CIS-611 -Final Project           ###
###                 May 6, 2016                ###
##################################################

#Get the first Q and parse it
import Opt_Choose_Best_Plan as op

executionStepsQ1 = []
with open("InputQ1.txt", "r") as reader:

    for line in reader:
        #print "line: ", line
        temp = []
        for word in line.split():
            #print "Word: ", word
            if word == "->":
                continue

            temp.append(word)
        executionStepsQ1.append(temp)

print executionStepsQ1

#Getthe Rewritten Optimized Query input
executionStepsRQ1 = []
with open("InputRQ1.txt", "r") as reader:

    for line in reader:
        #print "line: ", line
        temp = []
        for word in line.split():
            #print "Word: ", word
            if word == "->":
                continue

            temp.append(word)
        executionStepsRQ1.append(temp)

print executionStepsRQ1

Q1 = op.Opt_Choose_Best_Plan(executionStepsQ1)
RQ1 = op.Opt_Choose_Best_Plan(executionStepsRQ1)


print "Total Cost Q1: ", Q1.QueryPlan.totalCost
print "Execution Steps Q1: ", Q1.QueryPlan.steps

iterator = 0
print "----------Q1------------"

with open("InputQ1.txt", "r") as reader:

    for line in reader:
        if "Join" in line:
            print line.rstrip('\n'),"    ", Q1.QueryPlan.steps[iterator]
            iterator += 1
        else:
            print line.rstrip('\n')
iterator = 0
print "----------Q1------------"
with open("InputRQ1.txt", "r") as reader:

    for line in reader:
        if "Join" in line:
            print line.rstrip('\n'),"    ", RQ1.QueryPlan.steps[iterator]
            iterator += 1
        else:
            print line.rstrip('\n')




print "Total Cost RQ1: ", RQ1.QueryPlan.totalCost
print "Execution Steps RQ1", RQ1.QueryPlan.steps