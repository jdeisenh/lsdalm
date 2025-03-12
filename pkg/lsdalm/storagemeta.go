package streamgetter

// The format of the meta.json we store with the data
const StorageMetaFileName = "meta.json"

type StorageMeta struct {
	ManifestUrl string // Original manifest URL
	HaveMedia   bool   // Flag if we mirrored the media or not
}
