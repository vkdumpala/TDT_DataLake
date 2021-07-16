f = open("./ims_cls_claim.text","r")
schema = f.read()
f1 = open("./org_type_subtype_ims_cls_claim_DI.xml","a")
f1.write('<TableMapping table_type="Master">\n')
splitText = schema.split("^")
for i in splitText:
	splitColumn = i.split(" ")
	text = '<SourceColumn columnName=' + '"' + splitColumn[0] + '" ' + 'dataType=' + '"' + splitColumn[1] +  '" ' + 'nullable="true" length="200" Primarykey="false" scale="0" />'
	f1.write(text + '\n')
f1.write('</TableMapping>')



