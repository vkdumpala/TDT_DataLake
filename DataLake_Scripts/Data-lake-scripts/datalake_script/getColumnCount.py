f = open("./ims_cls_stage_info.txt","r")
schema = f.read()
count = 0
splitText = schema.split("^")
for i in splitText:
	count = count + 1
	print(i)
print(count)
