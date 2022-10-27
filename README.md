This is an implementation of Hitchhiker trees in Haskell meant to be translated to Sire.

This is meant to be used to implement databases in the Plunder system. Hitchhiker trees are a really good match for storage using Plunder pins to minimize the number of writes in snapshots.

Included is a demo which takes a bunch of included Derpibooru json data, builds an index of (tag -> id) using `HitchhikerSetMap` and allows searching on it. You can just type a search query at the prompt and get a list of ids which are the union of the images for each tag.

To get a dataset to try this on, you can grab this out of my dropbox:

```
$ wget https://www.dropbox.com/s/a9dwuiny135pk6o/datadir.tar.gz?dl=0 -O datadir.tar.gz
$ tar xvf datadir.tar.gz
```

(Or you could try `getdata.sh`, but that fetching endpoint is flaky.)

To run the program:

```
$ stack run -- datadir
SEARCH> twilight sparkle, rarity, fluttershy, tea
TAGS: ["twilight sparkle","rarity","fluttershy","tea"]
RESULT: fromList [2108132,2605433]
SEARCH> 
```
