  
### Exercice 8

def somme_rec(l) : 
    
    flatten_list = []
    for i in l :
        if type(i) == int :
            flatten_list.append(i)
        elif type(i) == list :
            for j in i :
                if type(j) == int :
                    flatten_list.append(j)
                elif type(j) == list :
                    for k in j : 
                        flatten_list.append(k)
                        
    return sum(flatten_list)

l = [3, [4, [2, 5]]]
print(somme_rec(l))
print(somme_rec([[[2], 1], [3, 4, [5]], 0]))


### Exercice 7


