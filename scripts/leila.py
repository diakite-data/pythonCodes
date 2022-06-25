#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 17 14:41:09 2021

@author: youssouf
"""

def instrus(Liste) :
      for i in Liste :
          if Liste.count(i) == 1:
              return i
         

def instrus1(Liste) :
    return [i for i in Liste if Liste.count(i) == 1]
            
         
def instrus2(Liste):
    l = Liste[0]
    L = []
    L1 = []
    for i in Liste :
        if i == l :
            L.append(i)
        else :
            L1.append(i)
    
    if len(L) == 1 :
        return L
    else :
        return L1
    
    


def duplicate_count(text) :
    text = text.lower()
    L = []
    for i in text : 
        if text.count(i) > 1 :
            L.append(i)
    return len(set(L))
    

    
def duplicate_count1(text) :
    return len(set([i for i in text.lower() if text.lower().count(i) > 1]))
