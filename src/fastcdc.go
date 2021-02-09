package main

import (
	"fmt"
	"io"

	"github.com/go-playground/validator/v10"
)

const (
	kiB = 1024
	miB = 1024 * 1024
)

type Options struct {
	minSize       uint `validate:"min=1"`
	maxSize       uint `validate:"min=0"` // Larger than min
	normSize      uint `validate:"min=0"` // Larger than min, less than max
	normalization int  `validate:"min=0,max=2"`
	seed          uint64
	bufferSize    int
}

type Chunk struct {
	offset      int
	length      int
	data        []byte
	fingerprint uint64
}

type Mask struct {
	// Empirically derived masks from paper
	L uint64
	S uint64
	A uint64
	// Masks that are calculated based on Rabin
	//  "...the lowest log2(D) bits of the hash value"
	// RabinL uint64
	// RabinS uint64
}

type Divider struct {
	minSize  uint
	maxSize  uint
	normSize uint
	masks    *Mask

	rd     io.Reader
	buffer []byte
	cursor int
	offset int
	eof    bool
}

// NewMask creates mask object for fastCDC()
func NewMask() *Mask {
	return &Mask{
		L: 0x0000d90003530000,
		S: 0x0003590703530000,
		A: 0x0000d90303530000,
	}
}


func NewOptions(min, max, norm uint) (Options, error) {
	// Populate object
	opts := &Options{
		minSize:  min,
		maxSize:  max,
		normSize: norm,
	}

	validate := validator.New()
	err := validate.Struct(opts)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}


	return opts, nil
}

// NewDivider creates an interface/object to chunk file
func NewDivider(rd io.Reader, opts Options) (*Divider, error) {

	// Check if options are valid
	validate := validator.New()
	err := validate.Struct(opts)
	if err != nil {
		fmt.Println(err.Error())
		return nil, err
	}

	// Instantiate masks
	mask := NewMask()

	// Populate object
	divider := &Divider{
		minSize:  opts.minSize,
		maxSize:  opts.maxSize,
		normSize: opts.normSize,
		masks:    mask,
		rd:       rd,
		buffer:   make([]byte, opts.bufferSize),
		cursor:   opts.bufferSize,
	}

	return divider, nil
}

func (d *Divider) populateBuffer() error {

	offset := len(d.buffer) - d.cursor

	// Reached end of buffer
	if offset >= int(d.maxSize) {
		return nil
	}

	// Buffer can be larger than maxSize, so if there is left over data
	// we must leftshift the remaining data to front and append new data
	copy(d.buffer[:offset], d.buffer[d.cursor:])
	d.cursor = 0

	if d.eof {
		d.buffer = d.buffer[:offset]
		return nil
	}

	//
	readBytes, err := io.ReadFull(d.rd, d.buffer[offset:])
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			d.buffer = d.buffer[:offset+readBytes]
			d.eof = true
		} else {
			return err
		}
	}

	return nil
}

// Next is the getter function for the iterator
func (d *Divider) Next() (Chunk, error) {

	err := d.populateBuffer()
	if err != nil {
		return Chunk{}, err
	}

	if len(d.buffer) == 0 {
		return Chunk{}, io.EOF
	}

	breakpoint, fp := d.fastCDC(d.buffer[d.cursor:])

	chunk := Chunk{
		offset: d.offset,
		length: breakpoint,
		data: d.buffer[d.cursor : d.cursor + breakpoint]
		fingerprint: fp,
	}

	d.cursor += breakpoint
	d.offset += breakpoint

	return chunk, nil
}

// Return chunking breakpoint, and hash
func (d *Divider) fastCDC(buffer []byte) (uint, uint64) {

	bufferLength := uint(len(buffer))
	breakpoint := d.minSize
	normSize := d.normSize

	// Rolling hash
	var fp uint64 = 0

	// If the buffer is too small
	if bufferLength <= d.minSize {
		return bufferLength, fp
	}

	// Truncate buffer to max length
	if bufferLength >= d.maxSize {
		bufferLength = d.maxSize
	} else if bufferLength <= normSize {
		normSize = bufferLength
	}

	// Find chunks smaller than normal size
	for ; breakpoint < normSize; breakpoint++ {
		fp = (fp << 1) + gear[buffer[breakpoint]]
		if (fp & d.masks.S) == 0 {
			return breakpoint, fp
		}
	}

	// Try to find any chunk smaller than maxSize
	for ; breakpoint < bufferLength; breakpoint++ {
		fp = (fp << 1) + gear[buffer[breakpoint]]
		if (fp & d.masks.L) == 0 {
			return breakpoint, fp
		}
	}

	return breakpoint, fp
}

/**
*	"It employs an array of 256 random 64-bit
*	integers to map the values of the byte contents
*	in the sliding window" (pg 3. FastCDC)
**/

/* the calculated bytes, whose size is the bit-width of the fp (hash) */
var gear [256]uint64 = [256]uint64{
	0xb0c077c3450c429d, 0xb402ed17a3cfa3b6, 0x443aadd91d707667, 0x80f4cb0d69384ba8,
	0x44d9b300095a47ce, 0xa3cb77638377d617, 0x7b7529b2f4405d8e, 0x45b10ec22ad5316e,
	0x99ab7cf0a151d4f5, 0xb46fff33d3634088, 0xef61829d6a3746ae, 0x0e99f268e322d4c4,
	0xd3a93a66652fb096, 0x1fa4078d9f94bed3, 0x92534d7dff508a49, 0xae58574ead8c0e81,
	0x18fe46a9724a30b0, 0x3111fabbf5be8353, 0x6b82a95a7ffda346, 0x326a5d56b1f66448,
	0xb331865f89d90f9e, 0x95636ac6b66fd115, 0x6081a8bd05ca21d0, 0xd7c4bb259932ee04,
	0xac847937c71fe2d7, 0x1cc29728f56e0c8c, 0xb612251fe0bf6514, 0x41483b01dfd5e5e2,
	0x5c64fd5534c74027, 0xecbe1b3ca233f409, 0xec932e40777c40fb, 0x420fad0989f0972b,
	0x87c7d5a0939445e0, 0xcc3ddf8ea5eb7bb9, 0x9c2df0ddbfb78588, 0x20db1ca7e58608ba,
	0x8a63b3b505bddd6b, 0xbc7b265ff0fcaa49, 0x253200a11bffa4d4, 0x3672cc4077670810,
	0x3c9d40c805a6e3f3, 0xd93e4eb68e059686, 0xee1d01938bf47140, 0x319754e31e522460,
	0xa201182502e2c90f, 0x5d7d3d82c40e8c3c, 0x43341fc8f7e9c797, 0xd3553cd686000738,
	0xc7f824f8101dec9b, 0x7e920430e8ab59a8, 0xebf154f9fa96bb3a, 0xc8440625584cb40b,
	0x2d715e54b277607a, 0xba0cfa726ad5243e, 0xb33c0cf48d060a7d, 0x399aa5a349144fde,
	0xb28b74a208a92da7, 0xd6f2bc9b3429b488, 0xf13a06fd9c6cd1dc, 0x62d0b8350896c8cc,
	0xb93a2a65f1f16044, 0xb275cb14fdbeb37e, 0xf4cbee02a270990c, 0xa19673fda0386d0d,
	0x4bbb7aa5abaab9d6, 0x08022d9d47e94c88, 0x3b3cd5d6062dc35d, 0x6fbbdb765c8bdecd,
	0x7b08a01eae9d88eb, 0x1198e0ada07e25c9, 0xc4f9f0dc607efa97, 0xc311c6bdd2f50d99,
	0x973e5cc0265a9694, 0xcf12638302f377e4, 0xaecc9748a7ce59bb, 0x98eb47f9944ee23c,
	0x878bc5bd4ae573d9, 0x4e3c3824ade7d361, 0xb3b11e4f6c9cd28f, 0x6b69267b69ffd3d6,
	0x01b496888b24f565, 0xd9fdcf794164b85c, 0xbb0b539a349ecd7c, 0xe3c28d04013f663d,
	0x8c5ea3eee1a1d00d, 0xefc82378a8540173, 0x675d558301e086db, 0x229b1f79c800356d,
	0x45c99e99e328dc2f, 0x811247270154c509, 0xb9b8b00ca7ddde1b, 0x769abcef10743e1f,
	0x89ef0fe6c1efdbed, 0x6c0914f4cf567912, 0xefdbeea5899c9f6e, 0x6e2460365a14879b,
	0xc1d720d5960bf021, 0x5e1d2c50cb1ce08d, 0x505e363c8b8de40a, 0x98f2495edc7a9510,
	0x6929f8669238cd59, 0x999250a001c027c8, 0xe6f6d540960486cb, 0x7d235eb88930e816,
	0xea9f282e975d84eb, 0x9055a327d0857966, 0x100335b26959d81f, 0xe60868cee2ab7a20,
	0x3ceee2bb0e4415f7, 0x35814c9c09146bd8, 0x867bf5df773a3813, 0xf29fc102d7a9c1da,
	0xfe4716e7ab43aee6, 0x05c68800ff4b4413, 0x174ea9fae69b350b, 0x0229cca707ce0ca8,
	0x18652f0f59966690, 0x6d4bbe9cc2330867, 0x0f3095d9b8c1a667, 0x7414f2dce05cf7f5,
	0x3c9c9fce0666baf3, 0x1406473b8b31845c, 0x0b731fa69cac6845, 0x44e8670789e85f78,
	0xfcd337237cd24f6e, 0xcb15686f1ea00b35, 0xe0b2b6e0695edef4, 0x546b07fc8dbecbde,
	0x92a1bf9d59c0a813, 0xe875b2d55ee9b12c, 0x963697c0341f91d1, 0x9985357a5ff958be,
	0x9b223f28de83bfbd, 0x0db9ef6c70201860, 0x78052bad894524e4, 0xc32729875ec22579,
	0xae0778f309a017e8, 0x8c846077be3b0a76, 0xe71a5a1699619578, 0x85a217bc6f4a270a,
	0x4c08345824936188, 0x7d57043f7964deb7, 0xaadb84994aea964b, 0x29850ec21d962e5a,
	0xe28e27374f136b6a, 0x094714d3df97d276, 0x5de34e1043898700, 0x8221f0a1ae994426,
	0x426a14e9f4852f49, 0xc501dd60facb6b87, 0x7b38ec1477d7d328, 0x80f902ac18dfffb5,
	0x623decb750125692, 0x0e3372e55c469097, 0xa336c690cd65ab2b, 0x2409d5a485a5033f,
	0x94c523e4c745f149, 0x383f532edea7459f, 0xd6cbbb70f0056bb6, 0x6cbd6c13cfb5cc30,
	0x29da1cee5a30d38c, 0x0696e01a8187821a, 0xd4a73ed8c86085fc, 0xc0047e2a5321500f,
	0xb3a48eb8324df512, 0x716f6e8df17a7729, 0x0298c4b27c03d89b, 0x9390974b6d6c0f49,
	0xdb4c203c8544b0b0, 0xf66bb519cc19356b, 0x8ff5cf56a9fd46d2, 0x7a1d952f42687932,
	0xb17efa6cedd84495, 0xc950b7f4e44339a5, 0x1c284733737f96fc, 0x66f22c66e703aa2b,
	0x5b827c4b3f8c08f3, 0x21ea7b3848f8516d, 0xd92defbbb9393549, 0xc26fff349006e967,
	0x444f9d0bc0b7fb08, 0xeee25c4192111527, 0x3f7c117c1f01cfa3, 0x44bd103f636d5dee,
	0x41ff28d681d84b31, 0x402819d8ab7c63a8, 0xb12e45d0107e86b0, 0xb2eaa46f66f68fb6,
	0xc446a787d47e29ff, 0xd9e4fd691ef1d52f, 0x63bdd9d5707e083e, 0x10433372a1d45713,
	0x73cb52272db50e1d, 0x1d057f136805ec8f, 0xfe460875ed6fd72c, 0x9f4aa9f53c0fec87,
	0x4b2606d1b6376f81, 0x23afc84abe9135e8, 0x44986b424c8e9c84, 0x6afa9800c5ab317b,
	0xe704a32c93dd9953, 0xc5e14b3faf00303f, 0xcab14bd58a1ce167, 0xb54c2cd2d30b8e8f,
	0xfe74a7f1bc6cae92, 0x050f0ebbe24ed74d, 0x41b2255aaa88e87d, 0x527c0590a8af7f7e,
	0x8fe44a04dc45bf78, 0xb8e953d118248d66, 0x95823d9e4e8e1a03, 0x824454bd1101d883,
	0x111e4e0d774e1ce2, 0x5926af09e5cf5bf7, 0xbdf7159df156d213, 0x51feaaa2235fd8b3,
	0xfa62f22aa1973bd1, 0x6139cf88d7a2ab3f, 0x77f30ba7b091c600, 0x37328bb973ddc894,
	0xf0f84b119c477900, 0x0e615a791273055f, 0x6dde759213ed939d, 0x6e9d5aeefe84e181,
	0x1574ff673ba5ea62, 0xeaa256226a51bf79, 0xd6794599234d9d28, 0x3b6e127276ed5d0c,
	0xcab65d5b219f512f, 0x6cc458b44d9f446e, 0x2cc85de8c75ad98b, 0x8ab7cf7260592b6d,
	0xb6df3434b42b6e4b, 0x666b4972cf8ca6d9, 0x63a3a8bafbd025ad, 0x4155e85263f8576a,
	0xf6547b6520b3c292, 0xd902765e4210d73f, 0xb9550aa264db4b4b, 0xfc8967c7472ea414,
	0x1e4480005f8bd736, 0x31938f83f42ebc23, 0xf7f184fa101f014b, 0x562abee3c5c03d0f,
	0xd26e924dad450a40, 0x568611cea25aa339, 0xd7fd41cefadecf79, 0x75024ba0009416ef,
	0xeb6c2b8ade7c00bd, 0x665ddb55f139bad1, 0x7e71e62b76fbe74a, 0xa61ddad86cdac7f4,
	0x37cf4311e7b14d9b, 0x988747e61cdfd266, 0x38b03788edc09ae9, 0x89aa3f745a253012,
}
