import csv
import os
import re
import sys
import sqlparse

META_FILE = "./files/metadata.txt"
AGGREGATE = ["min", "max", "sum", "avg"]

schema = {}
table_contents = {}

# create table schema
# -------------------
def metadata():
	try:
		with open(META_FILE,'r') as f:
			contents = f.readlines()
		contents = [t.strip() for t in contents if t.strip()]
		table_name = None
		for t in contents:
			if t=="<begin_table>":
				table_name = None
			elif t=="<end_table>":
				pass
			elif not table_name:
				table_name, schema[t] = t, []
			else:
				schema[table_name].append(t)
	except ValueError:
		print("error in reading data.")
		sys.exit()
# ---------------------------------------------------------------------------------------------------------------------

# populate table contents
# -----------------------
def read_data(dictionary):
	try:
		for i in dictionary:
			table_contents[i] = []
			with open('./files/'+i+'.csv') as csvfile:
				read = csv.reader(csvfile)
				for row in read:
					row = [j.replace("'",'') for j in row]
					row = [int(j) for j in row]
					table_contents[i].append(row)
	except ValueError:
		print("error populating data.")
		sys.exit()
# ---------------------------------------------------------------------------------------------------------------------

# joining two tables
# ------------------
def join_tables(table1,table2):
	a = []
	for i in table1:
		for j in table2:
			a.append(i+j)
	try:
		pass
	except ValueError:
		print("table not found.")
		sys.exit()
	return a
# ---------------------------------------------------------------------------------------------------------------------

# select query on joint tables
# ---------------------------- 
def select_part(tables,column_args):
	try:
		dictionary_headers = []
		for j in tables:
			for i in schema[j]:
				dictionary_headers.append(j+'.'+i)
		if "*" in column_args:
			column_args = dictionary_headers
		else:
			column_args = convert_argsv(column_args,tables)
		if column_args is None:
			return
		else:
			pass
		if len(tables)>1:
			p = join_tables(table_contents[tables[0]],table_contents[tables[1]])
			tables.remove(tables[0])
			tables.remove(tables[0])
			for i in tables:
				p = join_tables(p,table_contents[i])
		else:
			p = table_contents[tables[0]]
		
		indexes_printed = []
		for i in column_args:
			index = dictionary_headers.index(i)
			indexes_printed.append(index)

		fin_table = []
		for j in p:
			temp = []
			for i in indexes_printed:
				temp.append(j[i])
			fin_table.append(temp)
	except ValueError:
		print("error")
		sys.exit()
	return column_args, fin_table, p, dictionary_headers
# ---------------------------------------------------------------------------------------------------------------------

# print required output
# ---------------------
def print_particulars(cols_arg,fin_table):
	print(','.join(cols_arg))
	for i in fin_table:
		print(*i,sep=',')
# ---------------------------------------------------------------------------------------------------------------------

# create header dictionary
# -----------------------
def create_table_dictionary(table):
	a = []
	for i in schema[table]:
		a.append(table+'.'+i)
	return a
# ---------------------------------------------------------------------------------------------------------------------

# select distinct columns
# -----------------------
def select_distinct(columns,table):
	columns = re.split(r'[\ \t,]+',columns)
	temp = re.split(r'[\ \t,]+',table)
	if len(temp)>=2:
		print("multiple table distinct pairs not handled")
		sys.exit()
	p = create_table_dictionary(table)
	new_column = []
	for i in columns:
		if '.' not in i:
			if i in schema[table]:
				new_column.append(table+'.'+i)
			else:
				print("column "+i+" doesn't exist in the table.")
				sys.exit()
		else:
			if i not in p:
				print("column "+i+" doesn't exist in the table.")
				sys.exit()
			else:
				new_column.append(i)
	
	indices_needed = []
	print(','.join(new_column))
	
	for i in new_column:
		indices_needed.append(p.index(i))
	
	col_array = []
	for i in table_contents[table]:
		temp = []
		for j in indices_needed:
			temp.append(i[j])
		col_array.append(temp)
	col_array = [list(x) for x in set(tuple(x) for x in col_array)]
	for i in col_array:
		print(*i,sep=',')
# ---------------------------------------------------------------------------------------------------------------------

# check if a column exists in one or more tables with same name
# -------------------------------------------------------------
def find_multible_occurances(column,tables):
	p = 0
	table_name = ""
	for i in tables:
		if column in schema[i]:
			p = p+1
			table_name = i
	if p==1:
		return table_name
	elif p>1:
		return False
	else:
		print("column not found!")
		sys.exit()
# ---------------------------------------------------------------------------------------------------------------------

# convert table names to (table + '.' + col) format
# -------------------------------------------------
def convert_argsv(column_args,tables):
	new_col = []
	for i in column_args:
		if '.' not in i:
			check = find_multible_occurances(i,tables)
			if check=='ok':
				return None
			elif check==False:
				print("ambiguity!")
				sys.exit()
			else:
				new_col.append(check+'.'+i)
		else:
			new_col.append(i)
	return new_col
# ---------------------------------------------------------------------------------------------------------------------

# check aggregate functions
# -------------------------
def aggregate(col,dictionary,table,aggr):
	try:
		index = dictionary.index(col)
		column = []
		print(aggr+'('+col+')')
		for i in table:
			column.append(i[index])
		if aggr=="max":
			print(max(column))
		elif aggr=="min":
			print(min(column))
		elif aggr=="avg":
			print (sum(column)/float(len(column)))
		elif aggr=="sum":
			print (sum(column))
		else:
			print("error in aggregate format.")
			sys.exit()
	except ValueError:
		print("error in format")
		sys.exit()
# ---------------------------------------------------------------------------------------------------------------------

# check AND condition
# -------------------
def checkAnd(left,right,table_content,table_headers,tables):
	left_table = checkCondOps(left,table_content,table_headers,tables)
	right_table = checkCondOps(right,table_content,table_headers,tables)
	new_table = []
	for i in right_table:
		if i in left_table:
			new_table.append(i)
	return new_table
# ---------------------------------------------------------------------------------------------------------------------

# check OR condition
# ------------------
def checkOr(left,right,table_content,table_headers,tables):
	left_table = checkCondOps(left,table_content,table_headers,tables)
	right_table = checkCondOps(right,table_content,table_headers,tables)
	new_table, intersection = [] , []
	for i in right_table:
		if i in left_table:
			intersection.append(i)
	new_table = left_table+right_table
	for i in intersection:
		new_table.remove(i)
	return new_table
# ---------------------------------------------------------------------------------------------------------------------

# check if the condition has integer on one side
# ----------------------------------------------
def RepresentsInt(s):
	try: 
		int(s)
		return True
	except ValueError:
		return False
# ---------------------------------------------------------------------------------------------------------------------

# resolve condition if RHS and LLHS are table arguments
# -----------------------------------------------------
def both_table_vals(col1,col2,operator,table_headers,table_content):
	try:
		new_table = []
		index_cond1 = table_headers.index(col1)
		index_cond2 = table_headers.index(col2)
		for i in table_content:
			if operator == "<=":
				if i[index_cond1]<=i[index_cond2]:
					new_table.append(i)
			elif operator == ">=":
				if i[index_cond1]>=i[index_cond2]:
					new_table.append(i)
			elif operator == "=":
				if i[index_cond1]==i[index_cond2]:
					new_table.append(i)
			elif operator == ">":
				if i[index_cond1]>i[index_cond2]:
					new_table.append(i)
			elif operator == "<":
				if i[index_cond1]<i[index_cond2]:
					new_table.append(i)
		return new_table
	except ValueError:
		print("error")
		sys.exit()
# ---------------------------------------------------------------------------------------------------------------------

# resolve condition for int and table parts
# -----------------------------------------
def single_int_handler(arg,col_name,operator,table_headers,table_content):
	try:
		arg = int(arg)
		new_table = []
		index_cond = table_headers.index(col_name)
		for i in table_content:
			if operator == "<=":
				if i[index_cond]<=arg:
					new_table.append(i)
			elif operator == ">=":
				if i[index_cond]>=arg:
					new_table.append(i)
			elif operator == "=":
				if i[index_cond]==arg:
					new_table.append(i)
			elif operator == ">":
				if i[index_cond]>arg:
					new_table.append(i)
			elif operator == "<":
				if i[index_cond]<arg:
					new_table.append(i)
		return new_table
	except ValueError:
		print("error")
		sys.exit()
# ---------------------------------------------------------------------------------------------------------------------

# check condition operators
# -------------------------
def checkCondOps(condition,table_content,table_headers,tables):
	try:
		operator = None
		if "<=" in condition:
			condition = condition.split("<=")
			operator = "<="
		elif ">=" in condition:
			condition = condition.split(">=")
			operator = ">="
		elif "=" in condition:
			condition = condition.split("=")
			operator = "="
		elif ">" in condition:
			condition = condition.split(">")
			operator = ">"
		elif "<" in condition:
			condition = condition.split("<")
			operator = "<"
		else:
			print("error in condition")
			sys.exit()

		if RepresentsInt(condition[1]):
			if "." not in condition[0]:
				condition[0] = convert_arg_condition(condition[0],tables)
			return single_int_handler(condition[1],condition[0],operator,table_headers,table_content)
		else:
			if "." not in condition[0]:
				condition[0] = convert_arg_condition(condition[0],tables)
			if "." not in condition[1]:
				condition[1] = convert_arg_condition(condition[1],tables)
			return both_table_vals(condition[0],condition[1],operator,table_headers,table_content)
	except ValueError:
		print("error")
		sys.exit()
# ---------------------------------------------------------------------------------------------------------------------

# conversion
# ----------
def convert_arg_condition(name,tables):
	try:
		occur = 0
		new = ""
		for i in tables:
			if name in schema[i]:
				new = i+"."+name
				occur=occur+1
			if occur>1:
				print("ambiguity")
				sys.exit()
		if occur==0:
			print("column not exist")
			sys.exit()
		else:
			return new
	except ValueError:
		print("error")
		sys.exit()
# ---------------------------------------------------------------------------------------------------------------------

# check where condition
# ---------------------
def where(commands,columns,tables):
	try:
		if len(tables)>=2:
			table_names = []
			for i in tables:
				table_names.append(i)
			temp = select_part(tables,columns)
			joint_table, joint_table_dict = temp[2],temp[3]
			columns_needed = temp[0]

			if "AND" in commands:
				new_table = checkAnd(commands[0],commands[-1],joint_table,joint_table_dict,table_names)
				return print_new_table(new_table,columns_needed,joint_table_dict)
			elif "OR" in commands:
				new_table = checkOr(commands[0],commands[-1],joint_table,joint_table_dict,table_names)
				return print_new_table(new_table,columns_needed,joint_table_dict)
			else:
				new_table = checkCondOps(commands[0],joint_table,joint_table_dict,table_names)
				return print_new_table(new_table,columns_needed,joint_table_dict)
		
		elif len(tables)==1:
			columns_needed = convert_argsv(columns,tables)
			joint_table_dict = create_table_dictionary(tables[0])
			new_table = table_contents[tables[0]]

			if "AND" in commands:
				new_table = checkAnd(commands[0],commands[-1],new_table,joint_table_dict,tables)
				return print_new_table(new_table,columns_needed,joint_table_dict)
			elif "OR" in commands:
				new_table = checkOr(commands[0],commands[-1],new_table,joint_table_dict,tables)
				return print_new_table(new_table,columns_needed,joint_table_dict)
			else:
				new_table = checkCondOps(commands[0],new_table,joint_table_dict,tables)
				return print_new_table(new_table,columns_needed,joint_table_dict)
	except ValueError:
		print("error")
		sys.exit()
# ---------------------------------------------------------------------------------------------------------------------

# print table
# -----------
def print_new_table(table,column,joint_table_dict):
	indices = []
	print(','.join(column))
	for i in column:
		indices.append(joint_table_dict.index(i))
	for i in table:
		temp = []
		for j in indices:
			temp.append(i[j])
		print(*temp,sep=',')
	return
# ---------------------------------------------------------------------------------------------------------------------

# check commands for whitespace characters
# ----------------------------------------
def command_analyzer(commands):
	try:
		if ("AND" in commands and len(commands)==3) or ("OR" in commands and len(commands)==3):
			return commands
		elif "AND" in commands:
			fin_command = []
			index = commands.index("AND")
			cond1 = "".join(commands[:index])
			cond2 = "".join(commands[index+1:])
			fin_command.append(cond1)
			fin_command.append("AND")
			fin_command.append(cond2)
			return fin_command
		elif "OR" in commands:
			fin_command = []
			index = commands.index("OR")
			cond1 = "".join(commands[:index])
			cond2 = "".join(commands[index+1:])
			fin_command.append(cond1)
			fin_command.append("OR")
			fin_command.append(cond2)
			return fin_command
		else:
			fin_command = []
			fin_command.append("".join(commands))
			return fin_command
	except ValueError:
		print("error")
		sys.exit()
# ---------------------------------------------------------------------------------------------------------------------

# select what query needs to be executed
# --------------------------------------
def select_process(commands):
	try:
		# print(len(commands))
		if "where" in commands[-1]:
			strip_commands = commands[-1].replace(";","").split(" ")
			strip_commands = command_analyzer(strip_commands[1:])
			tables = re.split(r'[\ \t,]+',commands[3])
			cols_arg = re.split(r'[\ \t,]+',commands[1])
			if "*" in cols_arg:
				if len(cols_arg)==1:
					dictionary = []
					for i in tables:
						for j in schema[i]:
							dictionary.append(i+"."+j)
					cols_arg = dictionary
				else:
					print("column error")
					sys.exit()
			where(strip_commands,cols_arg,tables)
		elif commands[1].lower()=="distinct":
			select_distinct(commands[2],commands[4])
			return
		elif len(commands)==6 and "where" not in commands[-1]:
			print("Query not supported.")
			sys.exit()
		else:
			tables, cols_arg = commands[3], commands[1]
			check_aggr = cols_arg[:3]
			# print(check_aggr)
			if check_aggr in AGGREGATE and cols_arg[len(cols_arg)-1]==')':
				tables = re.split(r'[\ \t,]+',tables)
				dictionary = []
				for i in tables:
					for j in schema[i]:
						dictionary.append(i+"."+j)
				cols_arg = cols_arg.replace(check_aggr,"").replace("(","").replace(")","")
				# print(cols_arg)
				if "." not in cols_arg:
					cols_arg = convert_argsv(cols_arg,tables)[0]

				if len(tables)==1:
					new_table = (table_contents[tables[0]])
				elif len(tables)>=2:
					new_table = join_tables(table_contents[tables[0]],table_contents[tables[1]])
					tables.remove(tables[0])
					tables.remove(tables[0])
					for i in tables:
						new_table = join_tables(new_table,table_contents[i])
				else:
					print("input error")
					sys.exit()
				
				if cols_arg in dictionary:
					aggregate(cols_arg,dictionary,new_table,check_aggr)
				else:
					print("unknown error")
					sys.exit()			
			elif commands[2]=="from":
				cols_arg = re.split(r'[\ \t,]+',cols_arg)
				tables = re.split(r'[\ \t,]+',tables)
				reverted = select_part(tables,cols_arg)
				print_particulars(reverted[0],reverted[1])
			else:
				print("This query is not supported by slq-engine.")
				sys.exit()
			return
	except ValueError:
		print("error")
		sys.exit()
# ---------------------------------------------------------------------------------------------------------------------

# process query and parse
# -----------------------
def queryProcessor(query):
	if query[-1]!=';':
		print("Syntax Err: Expected ';' in the end")
		sys.exit()
	else:
		query_convert = sqlparse.parse(query)[0].tokens
		commands = []
		lst = sqlparse.sql.IdentifierList(query_convert).get_identifiers()
		for command in lst:
			commands.append(str(command))
		if commands[0].lower() == 'select':
			select_process(commands)
		else:
			print("This query is not supported by slq-engine.")
			sys.exit()
# ---------------------------------------------------------------------------------------------------------------------

# run main
#---------
def main():
	metadata()
	read_data(schema)
	if len(sys.argv) != 2:
		print("ERROR : invalid args")
		print("USAGE : python {} '<sql query>'".format(sys.argv[0]))
		exit(-1)
	queryProcessor(sys.argv[1])
# ---------------------------------------------------------------------------------------------------------------------

#run
#---
if __name__ == "__main__":
	main()
# ---------------------------------------------------------------------------------------------------------------------