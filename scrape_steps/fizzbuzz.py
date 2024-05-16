def fizzBuzz(n):
    chk3 = n/3
    chk5 = n/5
    
    str3 = ''
    str5 = ''
        
    
    if chk3.is_integer():
        str3 = 'Fizz'
    if chk5.is_integer():
        str5 = 'Buzz'
    
    
    if (str3 == '') and (str5 == ''):
        return print(str(n))
    
    return print(str3 + str5)

fizzBuzz(15)
