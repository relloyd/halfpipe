package shared

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/xo/dburl"
)

var DefaultDsnConnectionKeyNames = struct {
	Dsn string
}{
	Dsn: "dsn",
}

// DsnConnectionDetails is a simple struct to hold a DSN only.
type DsnConnectionDetails struct {
	Dsn            string `errorTxt:"data source name i.e. connect string" mandatory:"yes"`
	OriginalScheme string
}

// String returns the DSN with redacted password.
func (d DsnConnectionDetails) String() string {
	u, err := dburl.Parse(d.Dsn)
	if err != nil {
		panic(fmt.Sprintf("error parsing DSN %q: %v", d.Dsn, err))
	}
	return u.Redacted()
}

func (d DsnConnectionDetails) Parse() error {
	if d.Dsn == "" { // if the Dsn is invalid...
		return errors.New("DSN not found")
	}
	u, err := dburl.Parse(d.Dsn)
	if err != nil {
		return errors.Wrap(err, "DSN could not be parsed")
	}
	d.OriginalScheme = u.OriginalScheme // save the full connection type e.g. odbc+sqlserver, since pure sqlserver will be using the Go native module.
	return nil
}

func (d DsnConnectionDetails) GetScheme() (string, error) {
	if d.OriginalScheme == "" {
		if err := d.Parse(); err != nil {
			return "", err
		}
	}
	return d.OriginalScheme, nil
}

func (d DsnConnectionDetails) GetMap(m map[string]string) map[string]string {
	if m == nil {
		m = make(map[string]string)
	}
	m[DefaultDsnConnectionKeyNames.Dsn] = d.Dsn
	return m
}

// DsnConnectionDetailsToMap converts populates the supplied map with details from the connection-specific ODBC struct.
// The keys used to populate the map are those defaults held in DefaultDsnConnectionKeyNames.
// TODO: remove this by fixing 12factor code to use new methods above
func DsnConnectionDetailsToMap(m map[string]string, c *DsnConnectionDetails) map[string]string {
	if m == nil {
		m = make(map[string]string)
	}
	m[DefaultDsnConnectionKeyNames.Dsn] = c.Dsn
	return m
}

// GetDsnConnectionDetails converts generic ConnectionDetails to DsnConnectionDetails
// and returns a pointer to the new struct.
func GetDsnConnectionDetails(c *ConnectionDetails) *DsnConnectionDetails {
	return &DsnConnectionDetails{
		Dsn: c.Data[DefaultDsnConnectionKeyNames.Dsn],
	}
}
