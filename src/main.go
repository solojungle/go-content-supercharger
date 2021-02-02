package main

func main() {}

func generatePatches(dir string) {

	// We start by applying the content-defined chunking algorithm
	// to each file to determine which chunks it has.

	// Once we’ve determined the list of chunks, we hash and compress them.
	// The 64-bit hash value is used as the unique identifier of a chunk.
	// This allows us to quickly determine when a chunk is found in more than
	// one location so we can deduplicate it

	// For compressing chunks we use Zstandard. We compress using level 19,
	// which gives us very good compression ratios while keeping
	// decompression speeds high.

	// After compression, we bundle chunks together into a small
	// number of files that will end up on the CDN.

	// These bundles are just concatenations of related chunks that
	// clients will usually download in bulk. (ABCD.bundle)

	// We compress each chunk separately so clients can download
	// some chunks individually if that’s all they need.

	// Bundles are named after their unique 64-bit identifier.
	// We compute this ID based on the IDs of the chunks in them,
	// so two bundles with the same contents will have the same name

	// the next step is to write the release manifest.
	// The manifest stores information about all the
	// files, chunks, and bundles that are part of a release
	// and it’s about 8 MB.

	// The manifest uses the FlatBuffers binary format
	// to store this information

	// patchdata-service. This service runs in AWS and is
	// in charge of deploying the data to S3, which serves as
	// our global CDN origin.

	// Clients will only load a release manifest if it’s properly signed.
}
