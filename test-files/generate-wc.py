#!/usr/bin/env python
import sys

# generate random Gaussian values

from random import seed
from random import gauss

# seed random number generator

seed(sys.argv[1])

# generate some Gaussian values

words = """murder
suspend
time
spoil
polite
welcome
ladybug
cook
remind
changeable
caring
motion
thought
north
wealthy
ahead
sand
mouth
salty
terrible
abrasive
dreary
care
shock
steady
natural
scrub
apparel
reproduce
jellyfish
planes
sad
rain
hook
heap
greedy
lumber
double
attack
achiever
sprout
embarrassed
window
irate
unlock
best
fairies
car
rob
spare
increase
prepare
nine
bruise
fly
rude
station
cool
flow
retire
reading
deserted
elfin
circle
brush
enter
save
trees
connect
wave
daughter
sheet
leather
chief
riddle
wing
snatch
dirty
whip
moaning
pear
full
float
imaginary
produce
clumsy
box
ubiquitous
earth
quicksand
barbarous
feigned
laugh
suggest
verse
daily
squalid
bucket
wax
paltry"""

listofwords = words.split()
print(len(listofwords))
with open(sys.argv[2], 'a') as f:
	for i in range(150000):
		value = int(abs(gauss(0, 1)) / 3 * 100)
                if value >= 100:
			continue
		f.write(listofwords[value]+" ")
		if i > 0 and i % 20 == 0:
			f.write("\n")
f.close()
