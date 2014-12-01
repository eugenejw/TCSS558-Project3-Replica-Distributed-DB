import texttable as tt

tab = tt.Texttable()

x = [[]] # The empty row will have the header

for i in range(1,11):
    x.append([i,i**2,i**3, i])

tab.add_rows(x)
tab.set_cols_align(['r','r','r', 'r'])
tab.header(['Number', 'Number Squared', 'Number Cubed', 'FOURTH'])
print tab.draw()
