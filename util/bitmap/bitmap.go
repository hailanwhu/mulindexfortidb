package bitmap

import "log"

var rightmost_one_pos = []uint8{
	0, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	7, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	6, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	5, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
	4, 0, 1, 0, 2, 0, 1, 0, 3, 0, 1, 0, 2, 0, 1, 0,
}


type Bitmapset struct {
	nWords int
	words []uint32
}

func wordOffset(x int) int {
	return x / 32
}

func bitOffset(x int) int {
	return x % 32
}

func BitmapUnion(a, b *Bitmapset) *Bitmapset {
	var result,other *Bitmapset

	if a == nil {
		return BitmapCopy(b)
	}
	if b == nil {
		return BitmapCopy(a)
	}

	if a.nWords <= b.nWords {
		result = BitmapCopy(b)
		other = a
	} else {
		result = BitmapCopy(a)
		other = b
	}
	otherLen := other.nWords
	for i := 0; i < otherLen; i++ {
		result.words[i] |= other.words[i]
	}
	return result
}

func BitmapIntersect(a, b *Bitmapset) *Bitmapset {
	var result,other *Bitmapset

	if a == nil || b == nil {
		return nil
	}
	if a.nWords <= b.nWords {
		result = BitmapCopy(a)
		other = b
	} else {
		result = BitmapCopy(b)
		other = a
	}
	resultLen := result.nWords
	for i := 0; i < resultLen; i++ {
		result.words[i] &= other.words[i]
	}

	return result
}

func BitmapMakeSingleton(x int) *Bitmapset {
	if x < 0 {
		log.Print("x is negative")
		return nil
	}
	wordNum := wordOffset(x)
	bitNum := bitOffset(x)
	bm := new(Bitmapset)
	bm.nWords = wordNum + 1
	bm.words = make([]uint32, wordNum + 1, wordNum + 1)
	bm.words[wordNum] = 1 << uint32(bitNum)
	return bm
}

func  AddMember(bm *Bitmapset,x int) *Bitmapset {
	if x < 0 {
		log.Print("x is negative")
		return nil
	}

	if bm == nil {
		return BitmapMakeSingleton(x)
	}
	wordNum := wordOffset(x)
	bitNum := bitOffset(x)
	if wordNum >= bm.nWords {
		for i := bm.nWords; i < wordNum + 1; i++ {
			bm.words = append(bm.words, 0)
		}
		bm.nWords = wordNum + 1
	}

	bm.words[wordNum] |= 1 << uint32(bitNum)
	return bm
}

func BitmapCopy(bm *Bitmapset) *Bitmapset {
	newBm := new(Bitmapset)
	if bm == nil {
		return nil
	}
	newBm.nWords = bm.nWords
	newBm.words = make([]uint32, bm.nWords, bm.nWords)
	copy(newBm.words,bm.words)
	return newBm

}

func BitmapFindFirstMember(bm *Bitmapset) int{
	if bm == nil {
		return -1
	}
	nWords := bm.nWords
	for wordNum := 0; wordNum < nWords; wordNum++ {
		w := bm.words[wordNum]
		if w != 0 {
			w = uint32(int32(w) & (-(int32(w))))
			bm.words[wordNum] &= ^w

			result := wordNum * 32
			for w & 255 == 0  {
				w >>= 8
				result += 8
			}
			result += int(rightmost_one_pos[w & 255])
			return result
		}
	}
	return -1
}

func BitmapFindNextMember(bm *Bitmapset, prevBit int) int {
	if bm == nil {
		return -2
	}
	nWords := bm.nWords
	prevBit++
	mask := (^0) << uint32(bitOffset(prevBit))
	for wordNum := wordOffset(prevBit); wordNum < nWords; wordNum++ {
		w := bm.words[wordNum]
		w &= uint32(mask)

		if w != 0 {
			result := wordNum * 32
			for w & 255 == 0  {
				w >>= 8
				result += 8
			}
			result += int(rightmost_one_pos[w & 255])
			return result
		}

		mask = ^0

	}
	return -2
}

func IsABitmapMember(x int, bm *Bitmapset) bool{
	if x < 0 {
		return false
	}
	if bm == nil {
		return false
	}
	wordNum := wordOffset(x)
	bitNum := bitOffset(x)
	if wordNum >= bm.nWords {
		return false
	}
	if (bm.words[wordNum] & (1 << uint32(bitNum)))!=0 {
		return true
	}
	return false
}
