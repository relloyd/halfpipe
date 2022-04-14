package nzgo

import (
	"bufio"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	b64 "encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unsafe"

	"github.com/IBM/nzgo/oid"
)

// Common error types
var (
	ErrNotSupported              = errors.New("pq: Unsupported command")
	ErrInFailedTransaction       = errors.New("pq: Could not complete operation in a failed transaction")
	ErrSSLNotSupported           = errors.New("pq: SSL is not enabled on the server")
	ErrSSLKeyHasWorldPermissions = errors.New("pq: Private key file has group or world access. Permissions should be u=rw (0600) or less")
	ErrCouldNotDetectUsername    = errors.New("pq: Could not detect default username. Please provide one explicitly")

	errUnexpectedReady = errors.New("unexpected ReadyForQuery")
	errNoRowsAffected  = errors.New("no RowsAffected available after the empty statement")
	errNoLastInsertID  = errors.New("no LastInsertId available after the empty statement")
)

/* NPS handshake version negotiation packet structure */
type HsVersion struct {
	opcode  int
	version int
}

type DbosTupleDesc struct {
	version           int   /* CTable.m_version */
	nullsAllowed      int   /* CTable.nullsAllowed */
	sizeWord          int   /* CTable.m_sizeWord */
	sizeWordSize      int   /* CTable.m_sizeWordSize */
	numFixedFields    int   /* CTable.m_numFixedFields */
	numVaryingFields  int   /* CTable.m_numVaryingFields */
	fixedFieldsSize   int   /* CTable.m_fixedFieldsSize */
	maxRecordSize     int   /* CTable.m_maxRecordSize */
	numFields         int   /* CTable.m_numFields */
	field_type        []int /* field_t.type */
	field_size        []int /* field_t.size */
	field_trueSize    []int /* field_t.trueSize */
	field_offset      []int /* field_t.offset */
	field_physField   []int /* field_t.physField */
	field_logField    []int /* field_t.logField */
	field_nullAllowed []int /* field_t.nullAllowed */
	field_fixedSize   []int /* field_t.fixedSize */
	field_springField []int /* field_t.springField */
	DateStyle         int
	EuroDates         int
	DBcharset         int
	EnableTime24      int
}

type DATE_STRUCT struct {
	year  int
	month int
	day   int
}

type TIME_STRUCT struct {
	hour   uint16
	minute uint16
	second uint16
}

type timeStamp struct {
	tm_year int
	tm_mon  int
	tm_mday int
	tm_hour int
	tm_min  int
	tm_sec  int
}

type Interval struct {
	time  int /* all time units other than months and years */ // NZ - was double
	month int /* months and years, after time for alignment */
}

type TimeTzADT struct {
	time int // all time units other than months and years
	zone int // numeric time zone, in seconds
}

type TIMESTAMP_STRUCT struct {
	year     int
	month    int
	day      int
	hour     int
	minute   int
	second   int
	fraction int
}

// External table stuff (copied from nde/client/exttable.h)
const (
	EXTAB_SOCK_DATA  = 1 + iota // block of records
	EXTAB_SOCK_ERROR            // error message
	EXTAB_SOCK_DONE             // normal wrap-up
	EXTAB_SOCK_FLUSH            // Flush the current buffer/data
)

const (
	PGRES_EMPTY_QUERY = 0 + iota
	PGRES_COMMAND_OK  /* a query command that doesn't return */
	/* anything was executed properly by the backend */
	PGRES_TUPLES_OK /* a query command that returns tuples */
	/* was executed properly by the backend */
	PGRES_FIELDS_OK  /* field information from a query was successful */
	PGRES_END_TUPLES /* all is ok till here; all after this is error */
	PGRES_NONFATAL_ERROR
	PGRES_FATAL_ERROR
	PGRES_BAD_RESPONSE   /* an unexpected response was recv'd from the backend */
	PGRES_INTERNAL_ERROR /* memory allocation error in driver */
)

const (
	NzTypeRecAddr = 1 + iota // !NOTE-bmz need to add this to all switch stmts
	NzTypeDouble
	NzTypeInt
	NzTypeFloat
	NzTypeMoney
	NzTypeDate
	NzTypeNumeric
	NzTypeTime
	NzTypeTimestamp
	NzTypeInterval
	NzTypeTimeTz
	NzTypeBool
	NzTypeInt1
	NzTypeBinary
	NzTypeChar
	NzTypeVarChar
	NzDEPR_Text   // OBSOLETE 3.0: BLAST Era Large 'text' Object, (Postgres 'text' datatype overload, too)
	NzTypeUnknown // corresponds to PG UNKNOWNOID data type - an untyped string literal
	NzTypeInt2
	NzTypeInt8
	NzTypeVarFixedChar
	NzTypeGeometry
	NzTypeVarBinary
	NzDEPR_Blob // OBSOLETE 3.0: BLAST Era Large 'binary' Object
	NzTypeNChar
	NzTypeNVarChar
	NzDEPR_NText // OBSOLETE 3.0: BLAST Era Large 'nchar text' Object
	_            // skip 28
	_            // skip 29
	NzTypeJson   // 30
	NzTypeJsonb
	NzTypeJsonpath
	NzTypeLastEntry // KEEP THIS ENTRY LAST - used internally to size an array
)

const (
	CONN_NOT_CONNECTED = 0 + iota /* Connection has not been established */
	CONN_CONNECTED                /* Connection is up and has been established */
	CONN_EXECUTING                /* the connection is currently executing a statement */
	CONN_FETCHING                 /* the connection is currently executing a select */
	CONN_CANCELLED                /* the connection is currently cancelling a statement */
)

/* const to datatype string mapping to use in logger */
var dataType = map[int]string{
	NzTypeChar:         "NzTypeChar",
	NzTypeVarChar:      "NzTypeVarChar",
	NzTypeVarFixedChar: "NzTypeVarFixedChar",
	NzTypeGeometry:     "NzTypeGeometry",
	NzTypeVarBinary:    "NzTypeVarBinary",
	NzTypeNChar:        "NzTypeNChar",
	NzTypeNVarChar:     "NzTypeNVarChar",
	NzTypeJson:         "NzTypeJson",
	NzTypeJsonb:        "NzTypeJsonb",
	NzTypeJsonpath:     "NzTypeJsonpath",
}

const (
	CP_VERSION_1 = 1 + iota
	CP_VERSION_2
	CP_VERSION_3
	CP_VERSION_4
	CP_VERSION_5
	CP_VERSION_6
)

/* Client type */
const (
	NPS_CLIENT = 0 + iota
	IPS_CLIENT
)

type HSV2Msg struct {
	/* all message have a packet length (int) prepended
	 * the opcode len is included in the size.
	 */
	opcode  int
	payload string
}

/* Authentication types */
const (
	AUTH_REQ_OK = 0 + iota
	AUTH_REQ_KRB4
	AUTH_REQ_KRB5
	AUTH_REQ_PASSWORD
	AUTH_REQ_CRYPT
	AUTH_REQ_MD5
	AUTH_REQ_SHA256
)

/*
* This is used by the postmaster and clients in their handshake.
* This indicates type of information being exchanged between NPS and driver.
 */
const (
	HSV2_INVALID_OPCODE = 0 + iota
	HSV2_CLIENT_BEGIN
	HSV2_DB
	HSV2_USER
	HSV2_OPTIONS
	HSV2_TTY
	HSV2_REMOTE_PID
	HSV2_PRIOR_PID
	HSV2_CLIENT_TYPE
	HSV2_PROTOCOL
	HSV2_HOSTCASE
	HSV2_SSL_NEGOTIATE
	HSV2_SSL_CONNECT
	HSV2_APPNAME
	HSV2_CLIENT_OS
	HSV2_CLIENT_HOST_NAME
	HSV2_CLIENT_OS_USER
	HSV2_64BIT_VARLENA_ENABLED
)
const (
	HSV2_CLIENT_DONE = 1000 + iota
	HSV2_SERVER_BEGIN
	HSV2_PWD
	HSV2_SERVER_DONE = 2000
)

const (
	PG_PROTOCOL_3 = 3 + iota
	PG_PROTOCOL_4
	PG_PROTOCOL_5
)

//Client Type
const (
	NPSCLIENT_TYPE_GOLANG = 12
)

// Driver is the Postgres database driver.
type Driver struct{}

// Open opens a new connection to the database. name is a connection string.
// Most users should only use it through database/sql package from the standard
// library.
func (d *Driver) Open(name string) (c driver.Conn, err error) {
	c, err = Open(name)
	return c, err
}

func init() {
	sql.Register("nzgo", &Driver{})
}

type parameterStatus struct {
	// server version in the same format as server_version_num, or 0 if
	// unavailable
	serverVersion int

	// the current location based on the TimeZone value of the session, if
	// available
	currentLocation *time.Location
}

type transactionStatus byte

const (
	txnStatusIdle                transactionStatus = 'I'
	txnStatusIdleInTransaction   transactionStatus = 'T'
	txnStatusInFailedTransaction transactionStatus = 'E'
)

func (s transactionStatus) String() string {
	switch s {
	case txnStatusIdle:
		return "idle"
	case txnStatusIdleInTransaction:
		return "idle in transaction"
	case txnStatusInFailedTransaction:
		return "in a failed transaction"
	default:
		msg := fmt.Sprintf("unknown transactionStatus %d", s)
		return msg
	}
	msg := "not reached"
	elog.Debugln(msg)
	return msg
}

// Dialer is the dialer interface. It can be used to obtain more control over
// how pq creates network connections.
type Dialer interface {
	Dial(network, address string) (net.Conn, error)
	DialTimeout(network, address string, timeout time.Duration) (net.Conn, error)
}

type DialerContext interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

type defaultDialer struct {
	d net.Dialer
}

func (d defaultDialer) Dial(network, address string) (net.Conn, error) {
	return d.d.Dial(network, address)
}
func (d defaultDialer) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return d.DialContext(ctx, network, address)
}
func (d defaultDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return d.d.DialContext(ctx, network, address)
}

type conn struct {
	c         net.Conn
	buf       *bufio.Reader
	namei     int
	scratch   [2048]byte
	txnStatus transactionStatus
	txnFinish func()

	// Save connection arguments to use during CancelRequest.
	dialer Dialer
	opts   values

	// Cancellation key data for use with CancelRequest messages.
	processID int
	secretKey int

	parameterStatus parameterStatus

	saveMessageType   byte
	saveMessageBuffer []byte

	// If true, this connection is bad and all public-facing functions should
	// return ErrBadConn.
	bad bool

	// If set, this connection should never use the binary format when
	// receiving query results from prepared statements.  Only provided for
	// debugging.
	disablePreparedBinaryResult bool

	// Whether to always send []byte parameters over as binary.  Enables single
	// round-trip mode for non-prepared Query calls.
	binaryParameters bool

	// If true this connection is in the middle of a COPY
	inCopy bool

	//netezza specific
	hsVersion               int
	protocol1               int
	protocol2               int
	commandNumber           int
	status                  int
	guardium_clientHostName string
	guardium_clientOSUser   string
	guardium_applName       string
	guardium_clientOS       string
}

// Handle driver-side settings in parsed connection string.
func (cn *conn) handleDriverSettings(o values) (err error) {
	boolSetting := func(key string, val *bool) error {
		if value, ok := o[key]; ok {
			if value == "yes" {
				*val = true
			} else if value == "no" {
				*val = false
			} else {
				return elog.Fatalf(chopPath(funName()), "unrecognized value %q for %s", value, key)
			}
		}
		return nil
	}

	err = boolSetting("disable_prepared_binary_result", &cn.disablePreparedBinaryResult)
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return err
	}
	return boolSetting("binary_parameters", &cn.binaryParameters)
}

func (cn *conn) handlePgpass(o values) {
	// if a password was supplied, do not process .pgpass
	if _, ok := o["password"]; ok {
		return
	}
	filename := os.Getenv("PGPASSFILE")
	if filename == "" {
		// XXX this code doesn't work on Windows where the default filename is
		// XXX %APPDATA%\postgresql\pgpass.conf
		// Prefer $HOME over user.Current due to glibc bug: golang.org/issue/13470
		userHome := os.Getenv("HOME")
		if userHome == "" {
			user, err := user.Current()
			if err != nil {
				return
			}
			userHome = user.HomeDir
		}
		filename = filepath.Join(userHome, ".pgpass")
	}
	fileinfo, err := os.Stat(filename)
	if err != nil {
		return
	}
	mode := fileinfo.Mode()
	if mode&(0x77) != 0 {
		// XXX should warn about incorrect .pgpass permissions as psql does
		return
	}
	file, err := os.Open(filename)
	if err != nil {
		return
	}
	defer file.Close()
	scanner := bufio.NewScanner(io.Reader(file))
	hostname := o["host"]
	ntw, _ := network(o)
	port := o["port"]
	db := o["dbname"]
	username := o["user"]
	// From: https://github.com/tg/pgpass/blob/master/reader.go
	getFields := func(s string) []string {
		fs := make([]string, 0, 5)
		f := make([]rune, 0, len(s))

		var esc bool
		for _, c := range s {
			switch {
			case esc:
				f = append(f, c)
				esc = false
			case c == '\\':
				esc = true
			case c == ':':
				fs = append(fs, string(f))
				f = f[:0]
			default:
				f = append(f, c)
			}
		}
		return append(fs, string(f))
	}
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		split := getFields(line)
		if len(split) != 5 {
			continue
		}
		if (split[0] == "*" || split[0] == hostname || (split[0] == "localhost" && (hostname == "" || ntw == "unix"))) && (split[1] == "*" || split[1] == port) && (split[2] == "*" || split[2] == db) && (split[3] == "*" || split[3] == username) {
			o["password"] = split[4]
			return
		}
	}
}

func (cn *conn) writeBuf(b byte) *writeBuf {
	cn.scratch[0] = b
	return &writeBuf{
		buf: cn.scratch[:4],
		pos: 0,
	}
}

// Open opens a new connection to the database. dsn is a connection string.
// Most users should only use it through database/sql package from the standard
// library.
func Open(dsn string) (_ driver.Conn, err error) {
	return DialOpen(defaultDialer{}, dsn)
}

// DialOpen opens a new connection to the database using a dialer.
func DialOpen(d Dialer, dsn string) (_ driver.Conn, err error) {
	c, err := NewConnector(dsn)
	if err != nil {
		return nil, err
	}
	c.dialer = d
	return c.open(context.Background())
}

func (c *Connector) open(ctx context.Context) (cn *conn, err error) {
	o := c.opts

	cn = &conn{
		opts:   o,
		dialer: c.dialer,
	}

	err = cn.handleDriverSettings(o)
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return nil, err
	}
	cn.handlePgpass(o)

	cn.c, err = dial(ctx, c.dialer, o)
	if err != nil {
		if cn.c != nil {
			cn.c.Close()
		}
		elog.Fatalf(chopPath(funName()), err.Error())
		return nil, err
	}

	cn.buf = bufio.NewReader(cn.c)
	err = cn.startup(o)
	if err != nil {
		return nil, err
	}

	// reset the deadline, in case one was set (see dial)
	if timeout, ok := o["connect_timeout"]; ok && timeout != "0" {
		err = cn.c.SetDeadline(time.Time{})
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return cn, err
		}
	}

	return cn, nil
}

func dial(ctx context.Context, d Dialer, o values) (net.Conn, error) {
	network, address := network(o)
	// SSL is not necessary or supported over UNIX domain sockets
	if network == "unix" {
		o["sslmode"] = "disable"
	}

	elog.Debugln("Network ", network)
	elog.Debugln("Address ", address)

	// Zero or not specified means wait indefinitely.
	if timeout, ok := o["connect_timeout"]; ok && timeout != "0" {
		seconds, err := strconv.ParseInt(timeout, 10, 0)
		if err != nil {
			return nil, elog.Fatalf(chopPath(funName()), "invalid value for parameter connect_timeout: %s", err)
		}
		duration := time.Duration(seconds) * time.Second

		// connect_timeout should apply to the entire connection establishment
		// procedure, so we both use a timeout for the TCP connection
		// establishment and set a deadline for doing the initial handshake.
		// The deadline is then reset after startup() is done.
		deadline := time.Now().Add(duration)
		var conn net.Conn
		if dctx, ok := d.(DialerContext); ok {
			ctx, cancel := context.WithTimeout(ctx, duration)
			defer cancel()
			conn, err = dctx.DialContext(ctx, network, address)
		} else {
			conn, err = d.DialTimeout(network, address, duration)
		}
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return nil, err
		}
		err = conn.SetDeadline(deadline)
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return nil, err
		}
		return conn, nil
	}
	if dctx, ok := d.(DialerContext); ok {
		return dctx.DialContext(ctx, network, address)
	}
	return d.Dial(network, address)
}

func network(o values) (string, string) {
	host := o["host"]
	if strings.HasPrefix(host, "/") {
		sockPath := path.Join(host, ".s.PGSQL."+o["port"])
		return "unix", sockPath
	}

	return "tcp", net.JoinHostPort(host, o["port"])
}

type values map[string]string

// scanner implements a tokenizer for libpq-style option strings.
type scanner struct {
	s []rune
	i int
}

// newScanner returns a new scanner initialized with the option string s.
func newScanner(s string) *scanner {
	return &scanner{[]rune(s), 0}
}

// Next returns the next rune.
// It returns 0, false if the end of the text has been reached.
func (s *scanner) Next() (rune, bool) {
	if s.i >= len(s.s) {
		return 0, false
	}
	r := s.s[s.i]
	s.i++
	return r, true
}

// SkipSpaces returns the next non-whitespace rune.
// It returns 0, false if the end of the text has been reached.
func (s *scanner) SkipSpaces() (rune, bool) {
	r, ok := s.Next()
	for unicode.IsSpace(r) && ok {
		r, ok = s.Next()
	}
	return r, ok
}

// parseOpts parses the options from name and adds them to the values.
//
// The parsing code is based on conninfo_parse from libpq's fe-connect.c
func parseOpts(name string, o values) error {
	s := newScanner(name)

	for {
		var (
			keyRunes, valRunes []rune
			r                  rune
			ok                 bool
		)

		if r, ok = s.SkipSpaces(); !ok {
			break
		}

		// Scan the key
		for !unicode.IsSpace(r) && r != '=' {
			keyRunes = append(keyRunes, r)
			if r, ok = s.Next(); !ok {
				break
			}
		}

		// Skip any whitespace if we're not at the = yet
		if r != '=' {
			r, ok = s.SkipSpaces()
		}

		// The current character should be =
		if r != '=' || !ok {
			return elog.Fatalf(chopPath(funName()), `missing "=" after %q in connection info string"`, string(keyRunes))
		}

		// Skip any whitespace after the =
		if r, ok = s.SkipSpaces(); !ok {
			// If we reach the end here, the last value is just an empty string as per libpq.
			o[string(keyRunes)] = ""
			break
		}

		if r != '\'' {
			for !unicode.IsSpace(r) {
				if r == '\\' {
					if r, ok = s.Next(); !ok {
						return elog.Fatalf(chopPath(funName()), `missing character after backslash`)
					}
				}
				valRunes = append(valRunes, r)

				if r, ok = s.Next(); !ok {
					break
				}
			}
		} else {
		quote:
			for {
				if r, ok = s.Next(); !ok {
					return elog.Fatalf(chopPath(funName()), `unterminated quoted string literal in connection string`)
				}
				switch r {
				case '\'':
					break quote
				case '\\':
					r, _ = s.Next()
					fallthrough
				default:
					valRunes = append(valRunes, r)
				}
			}
		}

		o[string(keyRunes)] = string(valRunes)
	}

	err := elog.Initialize(o["logLevel"], o["logPath"], o["additionalLogFile"])
	if err != nil {
		return err
	}

	return nil
}

func (cn *conn) isInTransaction() bool {
	return cn.txnStatus == txnStatusIdleInTransaction ||
		cn.txnStatus == txnStatusInFailedTransaction
}

func (cn *conn) checkIsInTransaction(intxn bool) error {
	if cn.isInTransaction() != intxn {
		cn.bad = true
		return elog.Fatalf(chopPath(funName()), "unexpected transaction status %v", cn.txnStatus)
	}
	return nil
}

func (cn *conn) Begin() (_ driver.Tx, err error) {
	return cn.begin("")
}

func (cn *conn) begin(mode string) (_ driver.Tx, err error) {
	if cn.bad {
		return nil, driver.ErrBadConn
	}
	defer cn.errRecover(&err)

	err = cn.checkIsInTransaction(false)
	_, commandTag, err := cn.simpleExec("BEGIN")
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return nil, err
	}
	cn.txnStatus = txnStatusIdleInTransaction

	if commandTag != "BEGIN" {
		cn.bad = true
		return nil, elog.Fatalf(chopPath(funName()), "unexpected command tag %s", commandTag)
	}
	if cn.txnStatus != txnStatusIdleInTransaction {
		cn.bad = true
		return nil, elog.Fatalf(chopPath(funName()), "unexpected transaction status %v", cn.txnStatus)
	}
	return cn, nil
}

func (cn *conn) closeTxn() {
	if finish := cn.txnFinish; finish != nil {
		finish()
	}
}

func (cn *conn) Commit() (err error) {
	defer cn.closeTxn()
	if cn.bad {
		return driver.ErrBadConn
	}
	defer cn.errRecover(&err)

	err = cn.checkIsInTransaction(true)
	// We don't want the client to think that everything is okay if it tries
	// to commit a failed transaction.  However, no matter what we return,
	// database/sql will release this connection back into the free connection
	// pool so we have to abort the current transaction here.  Note that you
	// would get the same behaviour if you issued a COMMIT in a failed
	// transaction, so it's also the least surprising thing to do here.
	if cn.txnStatus == txnStatusInFailedTransaction {
		if err := cn.Rollback(); err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return err
		}
		return ErrInFailedTransaction
	}

	_, commandTag, err := cn.simpleExec("COMMIT")
	if err != nil {
		if cn.isInTransaction() {
			cn.bad = true
		}
		elog.Fatalf(chopPath(funName()), err.Error())
		return err
	}
	cn.txnStatus = txnStatusIdle
	if commandTag != "COMMIT" {
		cn.bad = true
		return elog.Fatalf(chopPath(funName()), "unexpected command tag %s", commandTag)
	}
	err = cn.checkIsInTransaction(false)
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return err
	}
	return nil
}

func (cn *conn) Rollback() (err error) {
	defer cn.closeTxn()
	if cn.bad {
		return driver.ErrBadConn
	}
	defer cn.errRecover(&err)

	err = cn.checkIsInTransaction(true)
	_, commandTag, err := cn.simpleExec("ROLLBACK")
	if err != nil {
		if cn.isInTransaction() {
			cn.bad = true
		}
		elog.Fatalf(chopPath(funName()), err.Error())
		return err
	}
	cn.txnStatus = txnStatusIdle
	if commandTag != "ROLLBACK" {
		return elog.Fatalf(chopPath(funName()), "unexpected command tag %s", commandTag)
	}
	err = cn.checkIsInTransaction(false)
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return err
	}
	return nil
}

func (cn *conn) gname() string {
	cn.namei++
	return strconv.FormatInt(int64(cn.namei), 10)
}

func (cn *conn) simpleExec(query string) (res driver.Result, commandTag string, err error) {

	var fname string
	var filename readBuf
	var fh *os.File

	if cn.status == CONN_EXECUTING || cn.status == CONN_FETCHING {
		cn.status = CONN_CONNECTED
		cn.Sock_clear_socket()
	} else if cn.status == CONN_CANCELLED {
		// Control will reach here only when the query was really huge and
		// even after Cancel request sent, it took too long to cancel and
		// Conn_clear_sock returned as data was not yet available
		cn.Sock_clear_socket()
	}

	elog.Infoln("Processing query:", query)
	var buffer *writeBuf

	if cn.commandNumber != -1 {
		cn.commandNumber++
		buffer = &writeBuf{
			buf: []byte{'P', '\x00', '\x00', '\x00', byte(cn.commandNumber)},
			pos: 1,
		}
		if cn.commandNumber > 100000 {
			cn.commandNumber = 1
		}
	}

	buffer.string(query)
	elog.Debugln(chopPath(funName()), "Buffer sent to nps: ", buffer.buf)

	_, err = cn.c.Write(buffer.buf)
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return emptyRows, commandTag, err
	}

	cn.status = CONN_EXECUTING

	for {
		var response byte
		response, err = cn.recvSingleByte()
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return emptyRows, commandTag, err
		}
		elog.Debugf(chopPath(funName()), "Backend response  %c \n", response)
		cn.recv_n_bytes(4)
		switch response {

		case 'C':
			length, _ := cn.recv_n_bytes(4)
			responseBuf, _ := cn.recv_n_bytes(int(length.int32()))
			res, commandTag, err = cn.parseComplete(responseBuf.string())
		case 'Z': /* Backend is ready for new query (6.4) */
			if err != nil {
				elog.Fatalf(chopPath(funName()), err.Error())
				return res, commandTag, err
			}
			return res, commandTag, nil
		case 'E':
			length, _ := cn.recv_n_bytes(4)
			responseBuf, err := cn.recv_n_bytes(int(length.int32()))
			errorString := responseBuf.string()
			err = errors.New(errorString)
			if err != nil {
				elog.Fatalf(chopPath(funName()), err.Error())
				return res, commandTag, err
			}
			return res, commandTag, nil
		case 'I':
			res = emptyRows
			if err != nil {
				elog.Fatalf(chopPath(funName()), err.Error())
				return res, commandTag, err
			}
			return res, commandTag, nil
		case 'N':
			length, _ := cn.recv_n_bytes(4)
			responseBuf, _ := cn.recv_n_bytes(int(length.int32()))
			elog.Infoln(funName(), responseBuf.string())
		case 'l':
			err := cn.xferTable()
			if err != nil {
				elog.Fatalf(chopPath(funName()), err.Error())
				return nil, commandTag, err
			}
			break
		case 'x': /* handle Ext Tbl parser abort */
			cn.recv_n_bytes(4)
			errorString := fmt.Sprintf("Error operation cancel")
			err = errors.New(errorString)
			if err != nil {
				elog.Fatalf(chopPath(funName()), err.Error())
				return res, commandTag, err
			}
			return res, commandTag, nil
			break
		case 'e':
			length, _ := cn.recv_n_bytes(4)
			logDir, _ := cn.recv_n_bytes(int(length.int32()))
			char, _ := cn.recvSingleByte()
			for char != 0 {
				filename = append(filename, char)
				char, _ = cn.recvSingleByte()
			}
			filename = append(filename, '\x00') /* null terminate it */
			logType, _ := cn.recv_n_bytes(4)
			if !(cn.getFileFromBE(logDir.string(), filename.string(), logType.int32())) {
				elog.Debugln(chopPath(funName()), "Error in writing file received from BE")
			}
			break
		case 'u': /* unload - initialize application protocol */
			// in ODBC, the first 10 bytes are utilized to populate clientVersion, formatType and bufSize
			// these are not needed in go lang, hence ignoring 10 bytes
			cn.recv_n_bytes(10)
			/* Next 16 bytes are Reserved Bytes for future extension*/
			cn.recv_n_bytes(16)
			/* Get the filename (specified in dataobject)*/
			fileSpecSize, _ := cn.recv_n_bytes(4)
			fname, _ := cn.recv_n_bytes(fileSpecSize.int32())
			fname = append(fname, '\x00') /* null terminate it */
			fh, err = os.OpenFile(fname.string(), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
			if err != nil { // file open failed
				// Report error to the client
				elog.Fatalf(chopPath(funName()), err.Error())
				return emptyRows, commandTag, err
			} else {
				// file open successfully, send status back to datawriter
				elog.Debugln(chopPath(funName()), "Successfully opened file: ", fh.Name())
				buf := []byte{'\x00', '\x00', '\x00', '\x00'}
				cn.c.Write(buf)
			}
		case 'U': /* handle unload data */
			err = cn.receiveAndWriteDatatoExternal(fname, fh)
		default:
			cn.bad = true
			errorString := fmt.Sprintf("Unknown response for simple exec: %q", response)
			err = errors.New(errorString)
			if err != nil {
				elog.Fatalf(chopPath(funName()), err.Error())
				return emptyRows, commandTag, err
			}
			return emptyRows, commandTag, nil
		}
	}
}

/* This is for unloading data recvd via named pipe spawned by datawriter */
func (cn *conn) receiveAndWriteDatatoExternal(filename string, file *os.File) error {

	cn.recv_n_bytes(4)
	allDone := false
	for !allDone {
		//  Get EXTAB_SOCK Status
		status, err := cn.recv_n_bytes(4)
		if err != nil {
			elog.Infof(chopPath(funName()), "Error while retrieving status, closing unload file: %q", err)
			// Close the file
			if err := file.Close(); err != nil {
				return elog.Fatalf(chopPath(funName()), "Unable to close the file: %q", err)
			}
			cn.Sock_clear_socket()
			elog.Fatalf(chopPath(funName()), err.Error())
			return err
		}
		switch status.int32() {

		case EXTAB_SOCK_DATA:
			// get number of bytes in block
			numBytes, _ := cn.recv_n_bytes(4)
			blockBuffer, _ := cn.recv_n_bytes(numBytes.int32())
			if _, err := file.Write([]byte(blockBuffer)); err != nil {
				return elog.Fatalf(chopPath(funName()), "Error in writing data to file: %q", err)
			} else {
				elog.Debugln(chopPath(funName()), "Successfully written data into file", file.Name())
			}
			break

		case EXTAB_SOCK_DONE:

			if err := file.Close(); err != nil {
				return elog.Fatalf(chopPath(funName()), "Unable to close the file: %q", err)
			}
			elog.Debugln(chopPath(funName()), "unload - done receiving data")
			allDone = true
			break

		case EXTAB_SOCK_ERROR:

			errNo, _ := cn.recv_n_bytes(2)
			len := errNo.int16()
			errorMsg, _ := cn.recv_n_bytes(len)

			errNo, _ = cn.recv_n_bytes(2)
			len = errNo.int16()
			errorObject, _ := cn.recv_n_bytes(len)

			elog.Infof(chopPath(funName()), "unload - ErrorMsg: %q, ErrorObj: %q", errorMsg, errorObject)

			// Close the file
			file.Close()
			return elog.Fatalf(chopPath(funName()), "unload - ErrorMsg: %q, ErrorObj: %q", errorMsg, errorObject)

		default:

			if err := file.Close(); err != nil {
				return elog.Fatalf(chopPath(funName()), "Unable to close the file: %q", err)
			}
			cn.Sock_clear_socket()
			return nil
		}
	}
	return nil
}

func (cn *conn) xferTable() error {

	cn.recv_n_bytes(4)
	var clientversion int = 1
	var filename readBuf
	var byteread int
	char, _ := cn.recvSingleByte()
	for char != 0 {
		filename = append(filename, char)
		char, _ = cn.recvSingleByte()
	}
	filename = append(filename, '\x00') /* null terminate it */
	hostversion, _ := cn.recv_n_bytes(4)

	_, _ = cn.c.Write([]byte{'\x00', '\x00', '\x00', byte(clientversion)})

	format, _ := cn.recv_n_bytes(4)
	blockSizebuf, _ := cn.recv_n_bytes(4)
	blockSize := blockSizebuf.int32()
	byteread = blockSize
	elog.Debugf(chopPath(funName()), "Format=%d Block size=%d Host version=%d ", format.int32(), blockSize, hostversion)

	filehandle, err := os.Open(filename.string())
	if err != nil { // file open failed
		return elog.Fatalf(chopPath(funName()), "Error opening file: %q", err)
	} else {

		elog.Debugln(chopPath(funName()), "Successfully opened External file to read: ", filehandle.Name())
		for blockSize == byteread {
			data := make([]byte, blockSize)
			byteread, _ = io.ReadFull(filehandle, data)
			length := make([]byte, 4)
			binary.BigEndian.PutUint32(length, uint32(byteread))
			data = append(append([]byte{'\x00', '\x00', '\x00', byte(EXTAB_SOCK_DATA)}, length...), data[:byteread]...)
			written, _ := cn.c.Write(data)
			elog.Debugln(chopPath(funName()), "No. of bytes sent to BE: ", written)
		}
		_, _ = cn.c.Write([]byte{'\x00', '\x00', '\x00', byte(EXTAB_SOCK_DONE)})
		elog.Debugln(chopPath(funName()), "sent EXTAB_SOCK_DONE to reader ")
	}
	return nil
}

/**************************************************************************
 * Function: getFileFromBE - This Routine opens a file in the temp directory
 *           using the filename specified by the BE in /tmp or c:\.
 *           The data sent by the BE are then written into this file.
 *
 * Parameters:
 *
 *  In       logDir - directory to put the file
 *           filename - name of file to write.
 *           logType - not used at this implementation.
 *
 *  Out      boolean - success or failure.
 *
 ****************************************************************************/
func (cn *conn) getFileFromBE(logDir string, filename string, logType int) bool {

	var status bool = true
	var fullpath string
	var fh *os.File
	var err error

	// If no explicit -logDir mentioned (defaulted by backend to /tmp)
	if runtime.GOOS == "windows" {
		fullpath = fmt.Sprintf("%s\\%s", logDir, filename)
	} else if runtime.GOOS == "linux" {
		fullpath = fmt.Sprintf("%s/%s", logDir, filename)
	}

	if logType == 1 {
		fullpath = fullpath + ".nzlog"
		fh, err = os.OpenFile(fullpath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	} else if logType == 2 {
		fullpath = fullpath + ".nzbad"
		fh, err = os.OpenFile(fullpath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	} else if logType == 3 {
		fullpath = fullpath + ".nzstats"
		fh, err = os.OpenFile(fullpath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	}

	if err != nil { // file open failed
		elog.Infof(chopPath(funName()), "Error opening file: %q", err)
		status = false
	}

	for {

		numBytesbuf, _ := cn.recv_n_bytes(4)
		numBytes := numBytesbuf.int32()

		if numBytes == 0 { // zeros means EOF, no more data
			break
		}

		dataBuffer, _ := cn.recv_n_bytes(numBytes)

		if status {
			if _, err := fh.Write([]byte(dataBuffer)); err != nil {
				elog.Infof(chopPath(funName()), "Error in writing data to file: %q", err)
				status = false
			} else {
				elog.Debugln(chopPath(funName()), "Successfully written data into file", fh.Name())
			}
		}
	}

	if err := fh.Close(); err != nil {
		elog.Infof(chopPath(funName()), "Unable to close the file: %q", err)
	}

	return status
}

func (cn *conn) connNextResultSet(query string) (res *rows, err error) {
	var filename readBuf
	for {
		response, err := cn.recvSingleByte()
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return nil, err
		}
		elog.Debugf(chopPath(funName()), "Backend response  %c \n", response)
		cn.recv_n_bytes(4)
		switch response {

		case 'A': /* Asynchronous Messages are ignored */
			fallthrough
		case 0: /* Ignored any null characters */
			fallthrough
		case 'L': /* This is receieved from server for batch queries after processing rows */
			break
		case 'C': /* portal query command, no tuples returned */
			length, _ := cn.recv_n_bytes(4)
			responseBuf, _ := cn.recv_n_bytes(int(length.int32()))
			resStr := fmt.Sprintf("%s", responseBuf.string())
			elog.Debugf(chopPath(funName()), "response received from backend: %s \n", resStr)
			break
		case 'Z':
			if err != nil {
				elog.Fatalf(chopPath(funName()), err.Error())
				return res, err
			}
			return res, nil
		case 'N':
			length, _ := cn.recv_n_bytes(4)
			responseBuf, _ := cn.recv_n_bytes(int(length.int32()))
			res = &rows{cn: cn}
			res.noticetag = responseBuf.string()
			elog.Debugf(chopPath(funName()), "notice received from backend: %s \n", res.noticetag)
			column := make([]string, 1)
			column[0] = "NOTICE"
			res.rowsHeader = rowsHeader{
				colNames: column,
			}
		case 'P': /* get the Portal name */
			length, _ := cn.recv_n_bytes(4)
			responseBuf, _ := cn.recv_n_bytes(int(length.int32()))
			elog.Debugf(chopPath(funName()), "response received from backend: %s \n", responseBuf.string())
			break
		case 'T':
			length, _ := cn.recv_n_bytes(4)
			responseBuf, err := cn.recv_n_bytes(int(length.int32()))
			res = &rows{cn: cn}
			res.rowsHeader = parsePortalRowDescribe(&responseBuf)
			if err != nil {
				elog.Fatalf(chopPath(funName()), err.Error())
				return res, err
			}
			return res, nil
		case 'I':
			if res == nil {
				res = &rows{
					cn: cn,
				}
			}
			res.done = true
			if err != nil {
				elog.Fatalf(chopPath(funName()), err.Error())
				return res, err
			}
			return res, nil
		case 'E':
			length, _ := cn.recv_n_bytes(4)
			responseBuf, err := cn.recv_n_bytes(int(length.int32()))
			errorString := responseBuf.string()
			err = errors.New(errorString)
			if err != nil {
				elog.Fatalf(chopPath(funName()), err.Error())
				return res, err
			}
			return res, nil
		case 'l':
			err := cn.xferTable()
			if err != nil {
				elog.Fatalf(chopPath(funName()), err.Error())
				return nil, err
			}
			break
		case 'x': /* handle Ext Tbl parser abort */
			cn.recv_n_bytes(4)
			errorString := fmt.Sprintf("Error operation cancel")
			err = errors.New(errorString)
			if err != nil {
				elog.Fatalf(chopPath(funName()), err.Error())
				return res, err
			}
			return res, nil
			break
		case 'e':
			length, _ := cn.recv_n_bytes(4)
			logDir, _ := cn.recv_n_bytes(int(length.int32()))
			char, _ := cn.recvSingleByte()
			for char != 0 {
				filename = append(filename, char)
				char, _ = cn.recvSingleByte()
			}
			filename = append(filename, '\x00') /* null terminate it */
			logType, _ := cn.recv_n_bytes(4)
			if !(cn.getFileFromBE(logDir.string(), filename.string(), logType.int32())) {
				elog.Debugln(chopPath(funName()), "Error in writing file received from BE")
			}
			break
		default:
			cn.bad = true
			errorString := fmt.Sprintf("Unexpected response: %q", response)
			err = errors.New(errorString)
			if err != nil {
				elog.Fatalf(chopPath(funName()), err.Error())
				return nil, err
			}
			return nil, nil
			break
		}

	}
}

func (cn *conn) simpleQuery(query string) (res *rows, err error) {

	defer cn.errRecover(&err)

	if cn.status == CONN_EXECUTING || cn.status == CONN_FETCHING {
		cn.status = CONN_CONNECTED
		cn.Sock_clear_socket()
	} else if cn.status == CONN_CANCELLED {
		// Control will reach here only when the query was really huge and
		// even after Cancel request sent, it took too long to cancel and
		// Conn_clear_sock returned as data was not yet available
		cn.Sock_clear_socket()
	}
	elog.Infoln("Processing query:", query)

	buffer := &writeBuf{
		buf: []byte{'P', '\xFF', '\xFF', '\xFF', '\xFF'},
		pos: 1,
	}

	if cn.commandNumber != -1 {
		cn.commandNumber++
		buffer = &writeBuf{
			buf: []byte{'P', '\x00', '\x00', '\x00', byte(cn.commandNumber)},
			pos: 1,
		}
		if cn.commandNumber > 100000 {
			cn.commandNumber = 1
		}
	}

	buffer.string(query)
	elog.Debugln(chopPath(funName()), "Buffer sent to nps: ", buffer.buf)

	_, err = cn.c.Write(buffer.buf)
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return nil, err
	}

	cn.status = CONN_EXECUTING

	return cn.connNextResultSet(query)

}

type noRows struct{}

var emptyRows noRows

var _ driver.Result = noRows{}

func (noRows) LastInsertId() (int64, error) {
	return 0, errNoLastInsertID
}

func (noRows) RowsAffected() (int64, error) {
	return 0, errNoRowsAffected
}

// Decides which column formats to use for a prepared statement.  The input is
// an array of type oids, one element per result column.
func decideColumnFormats(colTyps []fieldDesc, forceText bool) (colFmts []format, colFmtData []byte) {
	if len(colTyps) == 0 {
		return nil, colFmtDataAllText
	}

	colFmts = make([]format, len(colTyps))
	if forceText {
		return colFmts, colFmtDataAllText
	}

	allBinary := true
	allText := true
	for i, t := range colTyps {
		switch t.OID {
		// This is the list of types to use binary mode for when receiving them
		// through a prepared statement.  If a type appears in this list, it
		// must also be implemented in binaryDecode in encode.go.
		case oid.T_bytea:
			fallthrough
		case oid.T_int8:
			fallthrough
		case oid.T_int4:
			fallthrough
		case oid.T_int2:
			fallthrough
		case oid.T_varbinary:
			colFmts[i] = formatBinary
			allText = false

		default:
			allBinary = false
		}
	}

	if allBinary {
		return colFmts, colFmtDataAllBinary
	} else if allText {
		return colFmts, colFmtDataAllText
	} else {
		colFmtData = make([]byte, 2+len(colFmts)*2)
		binary.BigEndian.PutUint16(colFmtData, uint16(len(colFmts)))
		for i, v := range colFmts {
			binary.BigEndian.PutUint16(colFmtData[2+i*2:], uint16(v))
		}
		return colFmts, colFmtData
	}
}

func (cn *conn) prepareTo(query, stmtName string) (*stmt, error) {

	query = strings.ToLower(query)
	st := &stmt{cn: cn, name: stmtName, query: query}

	var placeholder string
	placeholder = "?"

	placeholderCount := strings.Count(query, placeholder)
	st.paramTyps = make([]oid.Oid, placeholderCount)

	query = strings.ReplaceAll(query, placeholder, "NULL")

	index := strings.Index(query, "select")
	if index != 0 {
		return st, nil
	}

	cn.Sock_clear_socket()
	elog.Infoln("Processing query:", query)
	buffer := &writeBuf{
		buf: []byte{'P', '\x00', '\x00', '\x00', byte(cn.commandNumber)},
		pos: 1,
	}
	buffer.string(query + " ANALYZE ")
	_, err := cn.c.Write(buffer.buf)
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return nil, err
	}

	for {
		response, err := cn.recvSingleByte()
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return nil, err
		}
		elog.Debugf(chopPath(funName()), "Backend response  %c \n", response)
		cn.recv_n_bytes(4)
		switch response {

		case 'P': /* get the Portal name */
			length, _ := cn.recv_n_bytes(4)
			responseBuf, _ := cn.recv_n_bytes(int(length.int32()))
			elog.Debugf(chopPath(funName()), "response received from backend: %s \n", responseBuf.string())
			break
		case 'T':
			length, _ := cn.recv_n_bytes(4)
			responseBuf, _ := cn.recv_n_bytes(int(length.int32()))
			st.rowsHeader = parsePortalRowDescribe(&responseBuf)
			return st, nil
		case 'E':
			length, _ := cn.recv_n_bytes(4)
			responseBuf, _ := cn.recv_n_bytes(int(length.int32()))
			return st, elog.Fatalf(chopPath(funName()), responseBuf.string())
		default:
			cn.bad = true
			return nil, elog.Fatalf(chopPath(funName()), "Unexpected response for analyze query: %q", response)
			break
		}
	}
	return st, nil
}

func (cn *conn) Prepare(q string) (_ driver.Stmt, err error) {
	if cn.bad {
		return nil, driver.ErrBadConn
	}
	defer cn.errRecover(&err)
	if len(q) >= 4 && strings.EqualFold(q[:4], "COPY") {
		s, err := cn.prepareCopyIn(q)
		if err == nil {
			cn.inCopy = true
		}
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return s, err
		}
		return s, nil
	}
	return cn.prepareTo(q, cn.gname())
}

func (cn *conn) Close() (err error) {
	// Skip cn.bad return here because we always want to close a connection.
	defer cn.errRecover(&err)
	// Ensure that cn.c.Close is always run. Since error handling is done with
	// panics and cn.errRecover, the Close must be in a defer.
	defer func() {
		cerr := cn.c.Close()
		if err == nil {
			err = cerr
		}
	}()

	// Don't go through send(); ListenerConn relies on us not scribbling on the
	// scratch buffer of this connection.
	return cn.sendSimpleMessage('X')
}

// Implement the "Queryer" interface
func (cn *conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return cn.query(query, args)
}

func (cn *conn) query(query string, args []driver.Value) (_ *rows, err error) {
	if cn.bad {
		return nil, driver.ErrBadConn
	}
	if cn.inCopy {
		return nil, errCopyInProgress
	}
	defer cn.errRecover(&err)
	// Check to see if we can use the "simpleQuery" interface, which is
	// *much* faster than going through prepare/exec
	if len(args) == 0 {
		return cn.simpleQuery(query)
	}

	if cn.binaryParameters {
		err = cn.sendBinaryModeQuery(query, args)
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return nil, err
		}
		err = cn.readParseResponse()
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return nil, err
		}
		err = cn.readBindResponse()
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return nil, err
		}
		rows := &rows{cn: cn}
		rows.rowsHeader, err = cn.readPortalDescribeResponse()
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return nil, err
		}
		err = cn.postExecuteWorkaround()
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return nil, err
		}
		return rows, nil
	}
	st, err := cn.prepareTo(query, "")
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return nil, err
	}
	st.Query(args)
	return &rows{
		cn:         cn,
		rowsHeader: st.rowsHeader,
	}, nil
}

// Implement the optional "Execer" interface for one-shot queries
func (cn *conn) Exec(query string, args []driver.Value) (res driver.Result, err error) {
	if cn.bad {
		return nil, driver.ErrBadConn
	}
	defer cn.errRecover(&err)
	// Check to see if we can use the "simpleExec" interface, which is
	// *much* faster than going through prepare/exec
	if len(args) == 0 {
		// ignore commandTag, our caller doesn't care
		r, _, err := cn.simpleExec(query)
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return r, err
		}
		return r, nil
	}

	if cn.binaryParameters {
		err = cn.sendBinaryModeQuery(query, args)
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return nil, err
		}
		err = cn.readParseResponse()
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return nil, err
		}
		err = cn.readBindResponse()
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return nil, err
		}
		_, err = cn.readPortalDescribeResponse()
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return nil, err
		}
		err = cn.postExecuteWorkaround()
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return nil, err
		}
		res, _, err = cn.readExecuteResponse("Execute")
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return nil, err
		}
		return res, nil
	}
	// Use the unnamed statement to defer planning until bind
	// time, or else value-based selectivity estimates cannot be
	// used.
	st, err := cn.prepareTo(query, "")
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return nil, err
	}
	r, err := st.Exec(args)
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return nil, err
	}
	return r, nil
}

func (cn *conn) send(m *writeBuf) error {
	elog.Debugln(chopPath(funName()), "Sock write buffer  ", m.wrap())
	//wrap function appends length of the data in int32 format
	_, err := cn.c.Write(m.wrap())
	if err != nil {
		elog.Infoln(chopPath(funName()), "Error : ", err)
		elog.Fatalf(chopPath(funName()), err.Error())
		return err
	}
	return nil
}

func (cn *conn) sendStartupPacket(m *writeBuf) error {
	_, err := cn.c.Write((m.wrap())[1:])
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return err
	}
	return nil
}

// Send a message of type typ to the server on the other end of cn.  The
// message should have no payload.  This method does not use the scratch
// buffer.
func (cn *conn) sendSimpleMessage(typ byte) (err error) {
	_, err = cn.c.Write([]byte{typ, '\x00', '\x00', '\x00', '\x04'})
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return err
	}
	return nil
}

// saveMessage memorizes a message and its buffer in the conn struct.
// recvMessage will then return these values on the next call to it.  This
// method is useful in cases where you have to see what the next message is
// going to be (e.g. to see whether it's an error or not) but you can't handle
// the message yourself.
func (cn *conn) saveMessage(typ byte, buf *readBuf) error {
	if cn.saveMessageType != 0 {
		cn.bad = true
		return elog.Fatalf(chopPath(funName()), "unexpected saveMessageType %d", cn.saveMessageType)
	}
	cn.saveMessageType = typ
	cn.saveMessageBuffer = *buf
	return nil
}

// recvMessage receives any message from the backend, or returns an error if
// a problem occurred while reading the message.
func (cn *conn) recvMessage(r *readBuf) (byte, error) {
	// workaround for a QueryRow bug, see exec
	if cn.saveMessageType != 0 {
		t := cn.saveMessageType
		*r = cn.saveMessageBuffer
		cn.saveMessageType = 0
		cn.saveMessageBuffer = nil
		return t, nil
	}
	x := cn.scratch[:7]
	_, err := io.ReadFull(cn.buf, x)
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return 0, err
	}

	// read the type and length of the message that follows
	t := x[0]
	n := int(binary.BigEndian.Uint32(x[1:])) - 4
	var y []byte
	if n <= len(cn.scratch) {
		y = cn.scratch[:n]
	} else {
		y = make([]byte, n)
	}
	_, err = io.ReadFull(cn.buf, y)
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return 0, err
	}
	*r = y
	return t, nil
}

// recv receives a message from the backend, but if an error happened while
// reading the message or the received message was an ErrorResponse, it will return error
// NoticeResponses are ignored.  This function should generally be used only
// during the startup sequence.
func (cn *conn) recv() (t byte, r *readBuf, err error) {
	for {
		var err error
		r = &readBuf{}
		t, err = cn.recvMessage(r)
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return t, r, err
		}

		switch t {
		case 'E':
			return t, r, parseError(r)
		case 'N':
			// ignore
		default:
			return t, r, nil
		}
	}
}

// recv1Buf is exactly equivalent to recv1, except it uses a buffer supplied by
// the caller to avoid an allocation.
func (cn *conn) recv1Buf(r *readBuf) byte {
	for {
		t, err := cn.recvMessage(r)
		if err != nil {
			elog.Debugln(chopPath(funName()), "Error while receiving message : ", err)
		}

		switch t {
		case 'A', 'N':
			// ignore
		case 'S':
			cn.processParameterStatus(r)
		default:
			return t
		}
	}
}

// recv1 receives a message from the backend, panicking if an error occurs
// while attempting to read it.  All asynchronous messages are ignored, with
// the exception of ErrorResponse.
func (cn *conn) recv1() (t byte, r *readBuf) {
	r = &readBuf{}
	t = cn.recv1Buf(r)
	return t, r
}

func (cn *conn) ssl(o values) error {
	upgrade, err := ssl(o)
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return err
	}

	if upgrade == nil {
		// Nothing to do
		return nil
	}

	w := cn.writeBuf(0)
	w.int32(80877103)
	if err = cn.sendStartupPacket(w); err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return err
	}

	b := cn.scratch[:1]
	_, err = io.ReadFull(cn.c, b)
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return err
	}

	if b[0] != 'S' {
		return ErrSSLNotSupported
	}

	cn.c, err = upgrade(cn.c)
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return err
	}
	return nil
}

// isDriverSetting returns true iff a setting is purely for configuring the
// driver's options and should not be sent to the server in the connection
// startup packet.
func isDriverSetting(key string) bool {
	switch key {
	case "host", "port":
		return true
	case "password":
		return true
	case "sslmode", "sslcert", "sslkey", "sslrootcert":
		return true
	case "fallback_application_name":
		return true
	case "connect_timeout":
		return true
	case "disable_prepared_binary_result":
		return true
	case "binary_parameters":
		return true

	default:
		return false
	}
}

func (cn *conn) recvSingleByte() (t byte, err error) {
	for {
		data := make([]byte, 1)
		nread, err := cn.c.Read(data[:])
		if nread == 0 {
			return data[0], elog.Fatalf(chopPath(funName()), "Single Byte Read failed; 0 bytes read")
		}
		if err != nil {
			return data[0], elog.Fatalf(chopPath(funName()), "Error reading single byte : %q", err)
		}
		return data[0], nil
	}
}

func (cn *conn) recv_n_bytes(n int) (r readBuf, err error) {
	for {
		var totalRead int = 0
		data := make([]byte, n)
		for totalRead < n {
			nread, err := cn.c.Read(data[totalRead:]) // it reads max 1024bytes in one go. Which also has handhsake data. If large data read is getting processed this is very imp
			if err != nil {
				return data, elog.Fatalln(chopPath(funName()), "Error reading n bytes : ", n, err)
			}
			totalRead = totalRead + nread
		}
		return data, nil
	}
}

func (cn *conn) startup(o values) (err error) {
	// Send the backend the name of the database we want to connect to, and the
	// user we want to connect as.  Additionally, we send over any run-time
	// parameters potentially included in the connection string.  If the server
	// doesn't recognize any of them, it will reply with an error.
	elog.Infoln("Starting handshake negotiation with server")
	versionPacket := HsVersion{
		opcode:  HSV2_CLIENT_BEGIN,
		version: CP_VERSION_6,
	}
	b := cn.writeBuf(0)
	b.int16(versionPacket.opcode)
	b.int16(versionPacket.version)
	elog.Debugln(chopPath(funName()), "Sending version ", versionPacket.version)
	err = cn.send(b)
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return err
	}
	//Handskhake negotiation with server
	for {
		beresp, _ := cn.recvSingleByte()
		elog.Debugf(chopPath(funName()), "Backend response  %c \n", beresp)
		if beresp == 'N' {
			cn.hsVersion = versionPacket.version
			cn.protocol2 = 0
			elog.Debugln(chopPath(funName()), "Exiting. Version (conn-protocol) = ", versionPacket.version)
			break
		} else if beresp == 'M' {
			/* Backend doesnt support this version */
			version, _ := cn.recvSingleByte()
			elog.Debugf(chopPath(funName()), "Version received from backend : %c \n", version)
			if version == '2' {
				/* Backend that support handshake version 2 return the version number
				* as a non-null erminated string. So in fact is sends the version as
				* char '2'.
				* The later backend return the version as an unsigned short int
				 */
				versionPacket.version = CP_VERSION_2
			} else if version == '3' {
				/* Backend that support handshake version 3 return the version number
				 * as a non-null erminated string. So in fact is sends the version as
				 * char '3'.
				 * The later backend return the version as an unsigned short int
				 */
				versionPacket.version = CP_VERSION_3
			} else if version == '4' {
				versionPacket.version = CP_VERSION_4
			} else if version == '5' {
				versionPacket.version = CP_VERSION_5
			}
			b = cn.writeBuf(0)
			b.int16(versionPacket.opcode)
			b.int16(versionPacket.version)
			elog.Debugln(chopPath(funName()), "Sending version ", versionPacket.version)
			err = cn.send(b)
			if err != nil {
				elog.Fatalf(chopPath(funName()), err.Error())
				return err
			}

		} else if beresp == 'E' {
			/* We no longer support the old startup packet approach for
			 * establishing connection
			 */
			return elog.Fatalf(chopPath(funName()), "Bad attribute value error")
		} else {
			return elog.Fatalf(chopPath(funName()), "Bad protocol error")
		}

	}
	elog.Infoln("Handshake negotiation successful")

	// guardium related information
	username, _ := user.Current()
	cn.guardium_clientOS = runtime.GOOS
	cn.guardium_clientOSUser = username.Username
	cn.guardium_clientHostName, err = os.Hostname()
	cn.guardium_applName = filepath.Base(os.Args[0])

	//Send handshake information to server
	elog.Infoln("Send handshake information to server")
	success, err := cn.Conn_send_database(o)
	if success != true {
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return err
		}
		return nil
	}

	success = cn.Conn_set_next_dataprotocol()
	if success != true {
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return err
		}
		return nil
	}

	success, err = cn.Conn_secure_session()
	if success != true {
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return err
		}
		return nil
	}

	switch cn.hsVersion {
	case CP_VERSION_6:
		fallthrough
	case CP_VERSION_4:
		success, err = cn.Conn_send_handshake_version4(o)
		break
	case CP_VERSION_5:
		fallthrough
	case CP_VERSION_3:
		success, err = cn.Conn_send_handshake_version2(o)
		break
	case CP_VERSION_2:
		success, err = cn.Conn_send_handshake_version2(o)
		break
	}
	if success != true {
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return err
		}
		return nil
	}

	//Authenticate the user
	success, err = cn.Conn_authenticate(o)
	if success != true {
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return err
		}
		return nil
	}

	//Restricted session related code
	cn.commandNumber = -1

	err = cn.Conn_send_query()
	if success != true {
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return err
		}
		return nil
	}

	cn.commandNumber = 0
	elog.Infoln("Connection successful !!")

	return nil
}

func (cn *conn) Conn_send_query() error {

	var query string

	rows, err := cn.simpleQuery("set nz_encoding to 'utf8'")
	if err != nil {
		return driver.ErrBadConn
	}

	/*	Set the Datestyle to the format the driver expects it to be in */
	if cn.opts["datestyle"] == "MDY" {
		query = "set DateStyle to 'US'"
	} else if cn.opts["datestyle"] == "DMY" {
		query = "set DateStyle to 'EUROPEAN'"
	} else {
		query = "set DateStyle to 'ISO'"
	}

	rows, err = cn.simpleQuery(query)
	if err != nil {
		return driver.ErrBadConn
	}

	// to be implemented : how to pass username, platform, client version etc in below query
	username, _ := user.Current()
	client_info := fmt.Sprintf("select version(), 'Netezza Golang Client Version: %s', '%s', 'OS Platform: %s', 'OS Username: %s'", nzgo_client_version, runtime.GOARCH, runtime.GOOS, username.Username)
	rows, err = cn.simpleQuery(client_info)
	if err != nil {
		return driver.ErrBadConn
	}
	noofcols := make([]driver.Value, len(rows.Columns()))
	rows.NextForCatalogueQuery(noofcols)

	client_info = fmt.Sprintf("SET CLIENT_VERSION = '%s'", nzgo_client_version)
	rows, err = cn.simpleQuery(client_info)
	if err != nil {
		return driver.ErrBadConn
	}

	rows, err = cn.simpleQuery("select ascii(' ') as space, encoding as ccsid from _v_database where objid = current_db")
	if err != nil {
		return driver.ErrBadConn
	}
	noofcols = make([]driver.Value, len(rows.Columns()))
	rows.NextForCatalogueQuery(noofcols)

	rows, err = cn.simpleQuery("select feature from _v_odbc_feature where spec_level = '3.5'")
	if err != nil {
		return driver.ErrBadConn
	}
	noofcols = make([]driver.Value, len(rows.Columns()))
	rows.NextForCatalogueQuery(noofcols)

	rows, err = cn.simpleQuery("select identifier_case, current_catalog, current_user")
	if err != nil {
		return driver.ErrBadConn
	}
	noofcols = make([]driver.Value, len(rows.Columns()))
	rows.NextForCatalogueQuery(noofcols)
	cn.commandNumber = 0

	rows.Close()
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return err
	}
	return nil
}

func (rs *rows) readTuplesForCatalogueQuery(dest []driver.Value) (b byte, err error) {

	conn := rs.cn
	response := conn.recv1Buf(&rs.rb)
	switch response {

	case 'D':
		for i := range dest {
			length := rs.rb.int32()
			length = length - 4
			dest[i], err = decode(&conn.parameterStatus, rs.rb.next(length), rs.colTyps[i].OID, rs.colFmts[i])
			if err != nil {
				elog.Fatalf(chopPath(funName()), err.Error())
				return response, err
			}
			elog.Debugln(chopPath(funName()), rs.rowsHeader.colNames[i], ":", dest[i])
		}
		response = rs.rb.byte()
		return response, nil
	default:
		return response, nil
	}
}

func (res *rows) NextForCatalogueQuery(dest []driver.Value) (err error) {

	if res.done {
		return io.EOF
	}

	cn := res.cn
	if cn.bad {
		return driver.ErrBadConn
	}
	defer cn.errRecover(&err)

	response, err := cn.recvSingleByte()
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return err
	}

	for {
		elog.Debugf(chopPath(funName()), "Backend response  %c \n", response)
		switch response {

		case 'C':
			elog.Debugln(chopPath(funName()), "All Rows fetched")
			res.done = true
			return io.EOF

		case 'D':
			cn.recv_n_bytes(7)
			length, _ := cn.recvSingleByte()
			cn.recvSingleByte()
			responseBuf, _ := cn.recv_n_bytes(int(length))
			elog.Debugln(chopPath(funName()), "Reading message from backend ", responseBuf)
			err = cn.saveMessage(response, &responseBuf)
			response, err = res.readTuplesForCatalogueQuery(dest)
			if err != nil {
				elog.Fatalf(chopPath(funName()), err.Error())
				return err
			}
			// for processing result set which return multiple rows
			for response == 68 {
				cn.recv_n_bytes(7)
				length, _ := cn.recvSingleByte()
				cn.recvSingleByte()
				responseBuf, _ := cn.recv_n_bytes(int(length))
				elog.Debugln(chopPath(funName()), "Reading message from backend ", responseBuf)
				err = cn.saveMessage(response, &responseBuf)
				response, err = res.readTuplesForCatalogueQuery(dest)
				if err != nil {
					elog.Fatalf(chopPath(funName()), err.Error())
					return err
				}
			}
			continue
		default:
			return elog.Fatalf(chopPath(funName()), "Unknown response: %d", response)
		}
	}

}

func convertDecimalToBinary(number byte) []byte {
	binary := make([]byte, 8)
	var remainder byte
	i := 0

	for number != 0 {
		remainder = number % 2
		number = number / 2
		binary[7-i] = remainder
		i++
	}
	return binary
}

func (rs *rows) readTuples(dest []driver.Value) (err error) {

	var bitmap []byte
	conn := rs.cn
	bitmaplen := len(dest) / 8
	if (len(dest) % 8) > 0 {
		bitmaplen++
	}
	response := conn.recv1Buf(&rs.rb)
	if response == 'D' {
		buffer := rs.rb.next(bitmaplen)
		for bitmaplen != 0 {
			decimal := *(*byte)(unsafe.Pointer(&buffer[bitmaplen-1]))
			binary := convertDecimalToBinary(decimal)
			bitmap = append(binary, bitmap...)
			bitmaplen--
		}
		for i := range dest {
			if bitmap[i] == 0 {
				dest[i] = nil
			} else {
				length := rs.rb.int32()
				length = length - 4
				dest[i], err = decode(&conn.parameterStatus, rs.rb.next(length), rs.colTyps[i].OID, rs.colFmts[i])
				if err != nil {
					elog.Fatalf(chopPath(funName()), err.Error())
					return err
				}
			}
			elog.Debugln(chopPath(funName()), rs.rowsHeader.colNames[i], ":", dest[i])
		}
	}
	return nil
}

func (res *rows) Next(dest []driver.Value) (err error) {
	if res.done {
		return io.EOF
	}

	cn := res.cn
	if cn.bad {
		return driver.ErrBadConn
	}
	defer cn.errRecover(&err)

	if res.noticetag != "" {
		dest[0] = res.noticetag
		res.done = true
		return
	}

	response, err := cn.recvSingleByte()
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return err
	}

	for {
		elog.Debugf(chopPath(funName()), "Backend response  %c \n", response)
		switch response {

		case 'C':
			elog.Debugln(chopPath(funName()), "All Rows fetched")
			re, _ := cn.connNextResultSet("")
			if re == nil {
				res.done = true
			} else {
				res.next = &re.rowsHeader
			}
			return io.EOF

		case 'D':
			cn.recv_n_bytes(4)
			length, _ := cn.recv_n_bytes(4)
			responseBuf, _ := cn.recv_n_bytes(int(length.int32()))
			elog.Debugln(chopPath(funName()), "Reading message from backend ", responseBuf)
			err = cn.saveMessage(response, &responseBuf)
			err = res.readTuples(dest)
			if err != nil {
				elog.Fatalf(chopPath(funName()), err.Error())
				return err
			}
			return
		case 'E':
			cn.recv_n_bytes(4)
			length, _ := cn.recv_n_bytes(4)
			responseBuf, _ := cn.recv_n_bytes(int(length.int32()))
			errorString := responseBuf.string()
			err = errors.New(errorString)
			elog.Infoln(funName(), errorString)
			if err != nil {
				elog.Fatalf(chopPath(funName()), err.Error())
				return err
			}
			return nil
		case 'Z': /* Backend is ready for new query (6.4) */
			if err != nil {
				elog.Fatalf(chopPath(funName()), err.Error())
				return err
			}
			return nil

		case 'X': //	get dbos tuple descriptor
			cn.recv_n_bytes(4)
			length, _ := cn.recv_n_bytes(4)
			responseBuf, _ := cn.recv_n_bytes(int(length.int32()))
			elog.Debugln(chopPath(funName()), "Reading message from backend ", responseBuf)
			res.Res_get_dbos_column_descriptions(&responseBuf)
			res.dbosTuple = true
			response, err = cn.recvSingleByte()
			break

		case 'Y': //	get dbos data tuple
			res.status = PGRES_TUPLES_OK
			res.Res_read_dbos_tuple(dest)
			return /* continue reading */

		case 0:
			res.done = true
			return io.EOF

		default:
			return elog.Fatalf(chopPath(funName()), "Unknown response: %d", response)
		}
	}

}

func CTable_FieldAt(tupdesc DbosTupleDesc, recP readBuf, field int) readBuf {
	/*    Assert(field < tupdesc->numFields); */
	if tupdesc.field_fixedSize[field] != 0 {
		return CTable_i_fixedFieldPtr(recP, tupdesc.field_offset[field])
	}
	return CTable_i_varFieldPtr(recP, tupdesc.fixedFieldsSize, tupdesc.field_offset[field])
}

func CTable_i_fixedFieldPtr(recP readBuf, offset int) readBuf {
	recP = recP[offset:]
	return recP
}

func CTable_i_varFieldPtr(recP readBuf, fixedOffset int, varDex int) readBuf {
	var lenP readBuf
	var ctr int
	var length int
	lenP = recP[fixedOffset:]
	for ctr = 0; ctr < varDex; ctr++ {
		length = int(binary.LittleEndian.Uint16(lenP))
		if length%2 == 0 {
			lenP = lenP[length:]
		} else {
			lenP = lenP[length+1:]
		}
	}
	return lenP
}

func CTable_i_fieldType(tupdesc DbosTupleDesc, coldex int) int {
	/*    Assert((coldex < tupdesc->numFields) && (coldex >= 0)); */
	return (tupdesc.field_type[coldex])
}

func CTable_i_fieldSize(tupdesc DbosTupleDesc, coldex int) int {
	/*    Assert((coldex < tupdesc->numFields) && (coldex >= 0)); */
	return (tupdesc.field_size[coldex])
}

func date2j(y int, m int, d int) int {

	var m12 int
	m12 = (m - 14) / 12

	return ((1461*(y+4800+m12))/4 + (367*(m-2-12*(m12)))/12 - (3*((y+4900+m12)/100))/4 + d - 32075)
} /* date2j() */

func j2date(jd int, year *int, month *int, day *int) {

	var j, y, m, d int
	var i, l, n int

	l = jd + 68569
	n = (4 * l) / 146097
	l -= (146097*n + 3) / 4
	i = (4000 * (l + 1)) / 1461001
	l += 31 - (1461*i)/4
	j = (80 * l) / 2447
	d = l - (2447*j)/80
	l = j / 11
	m = (j + 2) - (12 * l)
	y = 100*(n-49) + i + l

	*year = y
	*month = m
	*day = d
	return
} /* j2date() */

func time2struct(time int, ts *TIME_STRUCT) {

	time /= 1000000 // NZ microsecs

	ts.hour = (uint16)(time / 3600)
	time = time % 3600
	ts.minute = (uint16)(time / 60)
	ts.second = (uint16)(time % 60)
}

func IntervalToText(span *Interval) string {

	tm := timeStamp{
		tm_year: 0,
		tm_mon:  0,
		tm_mday: 0,
		tm_hour: 0,
		tm_min:  0,
		tm_sec:  0,
	}
	fsec := 0.0
	var neg_yflag, neg_dflag bool

	if interval2tm(span, &tm, &fsec, &neg_yflag, &neg_dflag) != 0 {
		return ""
	}

	fsec /= 1000000

	return EncodeTimeSpan(&tm, fsec)
}

func interval2tm(span *Interval, tm *timeStamp, fsec *float64, neg_yflag *bool, neg_dflag *bool) int {

	tmpVal := 0

	if span.month != 0 {
		tm.tm_year = span.month / 12
		tm.tm_mon = span.month % 12
	} else {
		tm.tm_year = 0
		tm.tm_mon = 0
	}

	if span.month < 0 {
		*neg_yflag = true
	}
	if span.time < 0 {
		*neg_dflag = true
	}

	time := span.time

	if time < 0 {
		tmpVal = int(math.Ceil(float64(time / 86400000000)))
	} else {
		tmpVal = int(math.Floor(float64(time / 86400000000)))
	}
	if tmpVal != 0 {
		time -= tmpVal * 86400000000
		tm.tm_mday = tmpVal
	}

	if time < 0 {
		tmpVal = int(math.Ceil(float64(time / 3600000000)))
	} else {
		tmpVal = int(math.Floor(float64(time / 3600000000)))
	}
	if tmpVal != 0 {
		time -= tmpVal * 3600000000
		tm.tm_hour = tmpVal
	}

	if time < 0 {
		tmpVal = int(math.Ceil(float64(time / 60000000)))
	} else {
		tmpVal = int(math.Floor(float64(time / 60000000)))
	}
	if tmpVal != 0 {
		time -= tmpVal * 60000000
		tm.tm_min = tmpVal
	}

	if time < 0 {
		tmpVal = int(math.Ceil(float64(time / 1000000)))
	} else {
		tmpVal = int(math.Floor(float64(time / 1000000)))
	}
	if tmpVal != 0 {
		time -= tmpVal * 1000000
		tm.tm_sec = tmpVal
	}

	*fsec = float64(time)

	return 0
}

func EncodeTimeSpan(tm *timeStamp, fsec float64) (str string) {

	/* The sign of year and month are guaranteed to match,
	 * since they are stored internally as "month".
	 * But we'll need to check for is_before and is_nonzero
	 * when determining the signs of hour/minute/seconds fields.
	 */

	var is_nonzero, is_before bool
	if tm.tm_year != 0 {

		str = fmt.Sprintf("%d year", tm.tm_year)
		if abs(tm.tm_year) != 1 {
			str = str + "s"
		} else {
			str = str + ""
		}
		is_before = (tm.tm_year < 0)
		is_nonzero = true

	}

	if tm.tm_mon != 0 {

		if is_nonzero == true {
			str = str + " "
		} else {
			str = str + ""
		}
		if is_before == true && (tm.tm_mon > 0) {
			str = str + "+"
		} else {
			str = str + ""
		}
		str = str + fmt.Sprintf("%d mon", tm.tm_mon)
		if abs(tm.tm_mon) != 1 {
			str = str + "s"
		} else {
			str = str + ""
		}

		is_before = (tm.tm_mon < 0)
		is_nonzero = true
	}

	if tm.tm_mday != 0 {

		if is_nonzero == true {
			str = str + " "
		} else {
			str = str + ""
		}
		if is_before == true && (tm.tm_mday > 0) {
			str = str + "+"
		} else {
			str = str + ""
		}
		str = str + fmt.Sprintf("%d day", tm.tm_mday)
		if abs(tm.tm_mday) != 1 {
			str = str + "s"
		} else {
			str = str + ""
		}

		is_before = (tm.tm_mday < 0)
		is_nonzero = true
	}

	if (is_nonzero == false) || (tm.tm_hour != 0) || (tm.tm_min != 0) || (tm.tm_sec != 0) || (fsec != 0) {

		minus := ((tm.tm_hour < 0) || (tm.tm_min < 0) || (tm.tm_sec < 0) || (fsec < 0))

		if is_nonzero == true {
			str = str + " "
		} else {
			str = str + ""
		}

		if minus == true {
			str = str + "-"
		} else {
			if is_before == true {
				str = str + "+"
			} else {
				str = str + ""
			}
		}

		str = str + fmt.Sprintf("%02d:%02d", abs(tm.tm_hour), abs(tm.tm_min))

		is_nonzero = true

		/* fractional seconds? */
		if fsec != 0 {
			fsec += float64(tm.tm_sec)
			str = str + fmt.Sprintf(":%09.6f", math.Abs(fsec))
			is_nonzero = true

			/* otherwise, integer seconds only? */
		} else if tm.tm_sec != 0 {
			str = str + fmt.Sprintf(":%02d", abs(tm.tm_sec))
			is_nonzero = true
		}
	}

	/* identically zero? then put in a unitless zero... */
	if is_nonzero == false {
		str = str + strconv.Itoa(0)
	}

	return str
} /* EncodeTimeSpan() */

func abs(n int) int {
	if n < 0 {
		return -n
	} else {
		return n
	}

}

func timetz_out_timetzadt(time_arg *TimeTzADT) string {

	tm := timeStamp{
		tm_year: 0,
		tm_mon:  0,
		tm_mday: 0,
		tm_hour: 0,
		tm_min:  0,
		tm_sec:  0,
	}

	time := time_arg.time / 1000000 // NZ microsecs
	fusec := (time_arg.time % 1000000)

	tm.tm_hour = (time / 3600)
	time = time % 3600
	tm.tm_min = (time / 60)
	tm.tm_sec = time % 60

	tz := time_arg.zone

	return EncodeTimeOnly(&tm, float64(fusec), tz)
}

/* EncodeTimeOnly()
 * Encode time fields only.
 */
func EncodeTimeOnly(tm *timeStamp, fusec float64, tzp int) (str string) {

	var hour, min int
	if (tm.tm_hour < 0) || (tm.tm_hour > 24) {
		return ""
	}

	if (tm.tm_min < 0) || (tm.tm_min > 59) {
		return ""
	}

	fusec /= 1000000

	str = fmt.Sprintf("%02d:%02d", tm.tm_hour, tm.tm_min)
	/* fractional seconds? */
	if fusec != 0 {
		fusec += float64(tm.tm_sec)
		str = str + fmt.Sprintf(":%09.6f", fusec)
		/* otherwise, integer seconds only? */
	} else if tm.tm_sec != 0 {
		str = str + fmt.Sprintf(":%02d", tm.tm_sec)

	}

	if tzp != 0 {

		hour = -(tzp / 3600)
		temp := tzp / 60

		if temp < 0 {
			temp = -temp
		}
		min = (temp % 60)

		if (hour == 0) && (tzp > 0) {
			str = str + fmt.Sprintf("-00:%02d", min)
		} else {
			if min != 0 {
				str = str + fmt.Sprintf("%+03d:%02d", hour, min)
			} else {
				str = str + fmt.Sprintf("%+03d", hour)
			}
		}
	}

	return str
} /* EncodeTimeOnly() */

func timestamp2struct(dt int, ts *TIMESTAMP_STRUCT) {

	date := dt / 86400000000
	date0 := date2j(2000, 1, 1)

	time := dt % 86400000000

	if time < 0 {
		time += 86400000000 // NZ - was 86400 w/o exp
		date -= 1
	}

	/* Julian day routine does not work for negative Julian days */
	if date < -date0 {
		return
	}

	/* add offset to go from J2000 back to standard Julian date */
	date += date0

	j2date(int(date), &ts.year, &ts.month, &ts.day)

	ts.fraction = (time % 1000000) // NZ microsecs
	/*
	* Netezza stores the fraction field of TIMESTAMP_STRUCT to
	* microsecond precision. The fraction field of a must be in
	* billionths, per ODBC spec. Therefore, multiply by 1000.
	 */
	ts.fraction *= 1000

	time /= 1000000 // NZ microsecs

	ts.hour = (time / 3600)
	time -= (ts.hour * 3600)
	ts.minute = (time / 60)
	ts.second = time - (ts.minute * 60)
}

func (res *rows) Res_get_dbos_column_descriptions(r *readBuf) {

	var ix int
	var tupdesc DbosTupleDesc

	tupdesc.version = r.int32()
	tupdesc.nullsAllowed = r.int32()
	tupdesc.sizeWord = r.int32()
	tupdesc.sizeWordSize = r.int32()
	tupdesc.numFixedFields = r.int32()
	tupdesc.numVaryingFields = r.int32()
	tupdesc.fixedFieldsSize = r.int32()
	tupdesc.maxRecordSize = r.int32()
	tupdesc.numFields = r.int32()

	tupdesc.field_type = make([]int, tupdesc.numFields)
	tupdesc.field_size = make([]int, tupdesc.numFields)
	tupdesc.field_trueSize = make([]int, tupdesc.numFields)
	tupdesc.field_offset = make([]int, tupdesc.numFields)
	tupdesc.field_physField = make([]int, tupdesc.numFields)
	// logicalField is unused information
	tupdesc.field_logField = make([]int, tupdesc.numFields)
	tupdesc.field_nullAllowed = make([]int, tupdesc.numFields)
	tupdesc.field_fixedSize = make([]int, tupdesc.numFields)
	// springField is unused information
	tupdesc.field_springField = make([]int, tupdesc.numFields)

	for ix = 0; ix < tupdesc.numFields; ix++ {
		tupdesc.field_type[ix] = r.int32()
		tupdesc.field_size[ix] = r.int32()
		tupdesc.field_trueSize[ix] = r.int32()
		tupdesc.field_offset[ix] = r.int32()
		tupdesc.field_physField[ix] = r.int32()
		tupdesc.field_logField[ix] = r.int32()
		tupdesc.field_nullAllowed[ix] = r.int32()
		tupdesc.field_fixedSize[ix] = r.int32()
		tupdesc.field_springField[ix] = r.int32()
	}

	tupdesc.DateStyle = r.int32()
	tupdesc.EuroDates = r.int32()
	if res.cn.protocol2 > PG_PROTOCOL_3 {
		tupdesc.DBcharset = r.int32()
	}
	if res.cn.protocol2 >= PG_PROTOCOL_5 {
		tupdesc.EnableTime24 = r.int32()
	}

	res.dbosTupleDescriptor = tupdesc
	return
}

func (res *rows) Res_read_dbos_tuple(dest []driver.Value) {

	// For alignment issues, the buffer is defined as Int8 array
	// as this is used for Int8s (in date-time data-types)
	conn := res.cn
	var field_lf, cur_field, workspace int
	var bitmap []byte

	numFields := res.dbosTupleDescriptor.numFields

	// The dbos tuple length
	conn.recv_n_bytes(8)
	reclenbuf, _ := conn.recv_n_bytes(4)
	reclen := int(reclenbuf.int32())

	// The dbos data tuple
	r, _ := conn.recv_n_bytes(reclen)

	if int(reclen) > res.dbosTupleDescriptor.maxRecordSize {
		res.dbosTupleDescriptor.maxRecordSize = int(reclen)
	}

	// bitmaplen denotes the number of bytes bitmap sent by backend. For e.g.: for select statement with
	// 9 columns, we would receive 2 bytes bitmap.

	bitmaplen := numFields / 8
	if (numFields % 8) > 0 {
		bitmaplen++
	}

	// We ignore first 2 bytes as that denotes length of message. Then in a loop we read one byte
	// at a time and convert it to binary bitmap.

	for l := 0; l < bitmaplen; l++ {
		binary := convertDecimalToBinary(r[2+l])
		bitmap = append(binary, bitmap...)
	}

	// reversing the bitmap
	for i, j := 0, len(bitmap)-1; i < j; i, j = i+1, j-1 {
		bitmap[i], bitmap[j] = bitmap[j], bitmap[i]
	}

	// The order of fields in Select list is same as pg-tuple order (for catalog queries)
	// same as in pg-tuple-descriptor loaded into IRD in Desc_read_column_descriptions (T message)
	// Also same as in DBOS-tuple-descriptor loaded into tupdesc in here (X message)
	// But order of fields in DBOS tuples will be in different order (Y messages)
	// This mapping (to physical position in dbos-tuple) is given by 'field_physField's of tupdesc
	// Ordering of fields in Dbos-tuple:
	//	All the fixed-size fields are packed at beginning of tuple (relative ordering could also change)
	//	Size of these fields together is 'fixedFieldsSize' (used as a starting-point for var-length fields)
	//	Variable length fields will be stored with first 2 byte-lengths, in any order

	// Regarding alignment of data, if there are no data-types that are of length 12 (8+4)
	// like Interval, TIMETZ, then from the field onwards data is aligned
	// hence there is no need to copy the data to workspace and used it
	// But if any such data-types are present, that come first, alignment is lost
	// FIXME - we can optimize reading-data for above mentioned scenario

	for field_lf = 0; field_lf < numFields && cur_field < numFields; field_lf++ {

		fieldDataP := CTable_FieldAt(res.dbosTupleDescriptor, r, cur_field)

		// a bitmap with value of 1 denotes null column
		if bitmap[res.dbosTupleDescriptor.field_physField[field_lf]] == 1 && res.dbosTupleDescriptor.nullsAllowed != 0 {
			dest[field_lf] = nil
			elog.Debugf(chopPath(funName()), "field=%d, value= NULL", cur_field+1)
			cur_field++
			continue
		}

		// Fldlen is byte-length of backend-datatype
		// memsize is byte-length of ODBC-datatype or internal-datatype for (Numeric/Interval)
		fldlen := CTable_i_fieldSize(res.dbosTupleDescriptor, cur_field)
		memsize := fldlen
		fldtype := CTable_i_fieldType(res.dbosTupleDescriptor, cur_field)

		switch fldtype {
		case NzTypeUnknown:
			fldtype = NzTypeVarChar
			fallthrough
		case NzTypeChar:
			fallthrough
		case NzTypeVarChar:
			fallthrough
		case NzTypeVarFixedChar:
			fallthrough
		case NzTypeGeometry:
			fallthrough
		case NzTypeVarBinary:
			memsize = memsize + 1
			break
		case NzTypeNChar:
			fallthrough
		case NzTypeNVarChar:
			fallthrough
		case NzTypeJson:
			fallthrough
		case NzTypeJsonb:
			fallthrough
		case NzTypeJsonpath:
			memsize *= 4
			memsize = memsize + 1 // for NULL-termination
			break
		case NzTypeDate:
			// converted to DATE struct from backend structure here itself
			memsize = 12
			break
		case NzTypeTime:
			// converted to TIME struct from backend structure here itself
			memsize = 8
			break
		case NzTypeInterval:
			// stored in backend format, but converted to string while retrieving
			memsize = 12
			break
		case NzTypeTimeTz:
			// converted to string from backend structure here itself
			memsize = 15
			break
		case NzTypeTimestamp:
			// converted to TIMESTAMP struct from backend structure here itself
			memsize = 8
			break
		case NzTypeBool:
			memsize = 1
		default:
			break
			// For all other data-types backend-format is same as ODBC-format
			// Hence fldlen == memsize
		}

		// FIXME: This memory allocation for tuples is freshly done for each batch
		// and freed before the next batch start. This is unneccessary, as we are
		// allocating MAX-size of each data-type. This should be done once-for-all-batchs
		// Hence should be moved to getDbosTupleDescriptor function
		// NOTE: With LOB support, we should be using seperate alloc-blocks for LOBs
		// and NOT allow rowset-size and cache-size to be greater than 1, as it woud not
		// be feasible to cache GBs/TBs of data; GetData should directly fetch the data
		// from the socket.

		switch fldtype {
		case NzTypeChar:
			dest[field_lf] = ""
			byteBuf := make([]byte, fldlen)
			copy(byteBuf, fieldDataP.next(fldlen)) //make a copy

			dest[field_lf] = string(byteBuf)
			elog.Debugf(chopPath(funName()), "field=%d, datatype=CHAR, value=%s, len=%d ", cur_field+1, dest[field_lf], fldlen)

		case NzTypeNChar:
			fallthrough
		case NzTypeVarFixedChar:
			cursize := int(binary.LittleEndian.Uint16(fieldDataP)) - 2 //to ignore 2 bytes
			fieldDataP.next(2)                                         //ignoring 2 bytes
			dest[field_lf] = ""
			byteBuf := make([]byte, cursize)
			copy(byteBuf, fieldDataP.next(cursize)) //make a copy

			for cursize < fldlen {
				byteBuf = append(byteBuf, ' ')
				cursize++
			}
			dest[field_lf] = string(byteBuf)
			elog.Debugf(chopPath(funName()), "field=%d, datatype=%s, value=%s, len=%d ", cur_field+1, dataType[fldtype], dest[field_lf], fldlen)

		case NzTypeVarChar:
			fallthrough
		case NzTypeNVarChar:
			fallthrough
		case NzTypeGeometry:
			fallthrough
		case NzTypeVarBinary:
			fallthrough
		case NzTypeJson:
			fallthrough
		case NzTypeJsonb:
			fallthrough
		case NzTypeJsonpath:
			cursize := int(binary.LittleEndian.Uint16(fieldDataP)) - 2 //to ignore 2 bytes
			fieldDataP.next(2)                                         //ignoring 2 bytes
			dest[field_lf] = ""
			byteBuf := make([]byte, cursize)
			copy(byteBuf, fieldDataP.next(cursize)) //make a copy

			dest[field_lf] = string(byteBuf)
			fldlen = cursize
			elog.Debugf(chopPath(funName()), "field=%d, datatype=%s, value=%s, len=%d ", cur_field+1, dataType[fldtype], dest[field_lf], fldlen)

		case NzTypeInt8: //int64
			byteBuf := fieldDataP.next(fldlen)
			dest[field_lf] = *(*int64)(unsafe.Pointer(&byteBuf[0]))
			elog.Debugf(chopPath(funName()), "field=%d, datatype=NzTypeInt8, value=%d, len=%d ", cur_field+1, dest[field_lf], fldlen)

		case NzTypeInt: //int32
			byteBuf := fieldDataP.next(fldlen)
			dest[field_lf] = *(*int32)(unsafe.Pointer(&byteBuf[0]))
			elog.Debugf(chopPath(funName()), "field=%d, datatype=NzTypeInt(Int4), value=%d, len=%d ", cur_field+1, dest[field_lf], fldlen)

		case NzTypeInt2: //int16
			byteBuf := fieldDataP.next(fldlen)
			dest[field_lf] = *(*int16)(unsafe.Pointer(&byteBuf[0]))
			elog.Debugf(chopPath(funName()), "field=%d, datatype=NzTypeInt2, value=%d, len=%d ", cur_field+1, dest[field_lf], fldlen)

		case NzTypeInt1: //int8
			byteBuf := fieldDataP.next(fldlen)
			dest[field_lf] = *(*int8)(unsafe.Pointer(&byteBuf[0]))
			elog.Debugf(chopPath(funName()), "field=%d, datatype=NzTypeInt1, value=%d, len=%d ", cur_field+1, dest[field_lf], fldlen)

		case NzTypeDouble: //double precision 64bits
			byteBuf := fieldDataP.next(fldlen)
			bits := (*(*uint64)(unsafe.Pointer(&byteBuf[0])))
			dest[field_lf] = fmt.Sprintf("%.15g", math.Float64frombits(bits))
			elog.Debugf(chopPath(funName()), "field=%d, datatype=NzTypeDouble, value=%s, len=%d ", cur_field+1, dest[field_lf], fldlen)

		case NzTypeFloat: //double precision 32bits
			byteBuf := fieldDataP.next(fldlen)
			bits := (*(*uint32)(unsafe.Pointer(&byteBuf[0])))
			dest[field_lf] = math.Float32frombits(bits)
			elog.Debugf(chopPath(funName()), "field=%d, datatype=NzTypeFloat, value=%f, len=%d ", cur_field+1, dest[field_lf], fldlen)

		case NzTypeDate:
			{
				date_value := DATE_STRUCT{
					year:  0,
					month: 0,
					day:   0,
				}
				byteBuf := fieldDataP[:fldlen]
				workspace = int(*(*int32)(unsafe.Pointer(&byteBuf[0])))
				j2date((workspace + date2j(2000, 1, 1)), &date_value.year, &date_value.month, &date_value.day)
				dest[field_lf] = fmt.Sprintf("%02d-%02d-%02d", date_value.year, date_value.month, date_value.day)
				elog.Debugf(chopPath(funName()), "field=%d, datatype=DATE, value=%s, len=%d ", cur_field+1, dest[field_lf], fldlen)
				//fldlen = memsize
			}
			break

		case NzTypeTime:
			{
				time_value := TIME_STRUCT{
					hour:   0,
					minute: 0,
					second: 0,
				}
				workspace = int(binary.LittleEndian.Uint64(fieldDataP[:fldlen]))
				time2struct(workspace, &time_value)
				dest[field_lf] = fmt.Sprintf("%02d:%02d:%02d", int(time_value.hour), int(time_value.minute), int(time_value.second))
				elog.Debugf(chopPath(funName()), "field=%d, datatype=TIME, value=%s, len=%d ", cur_field+1, dest[field_lf], fldlen)
				fldlen = memsize
			}
			break

		case NzTypeInterval:

			interval := Interval{
				time:  0,
				month: 0,
			}
			interval.time = int(binary.LittleEndian.Uint64(fieldDataP[:fldlen-4]))
			byteBuf := fieldDataP[fldlen-4 : fldlen]
			interval.month = int(*(*int32)(unsafe.Pointer(&byteBuf[0])))
			dest[field_lf] = IntervalToText(&interval)
			elog.Debugf(chopPath(funName()), "field=%d, datatype=INTERVAL, value=%s, len=%d ", cur_field+1, dest[field_lf], fldlen)
			break

		case NzTypeTimeTz:
			timetz_value := TimeTzADT{
				time: 0,
				zone: 0,
			}

			timetz_value.time = int(binary.LittleEndian.Uint64(fieldDataP[:fldlen-4]))
			byteBuf := fieldDataP[fldlen-4 : fldlen]
			timetz_value.zone = int(*(*int32)(unsafe.Pointer(&byteBuf[0])))
			/*** convert to TIME_STRUCT ***/
			dest[field_lf] = timetz_out_timetzadt(&timetz_value)
			elog.Debugf(chopPath(funName()), "field=%d, datatype=TIMETZ, value=%s, len=%d ", cur_field+1, dest[field_lf], fldlen)
			break

		case NzTypeTimestamp:
			{
				timestamp_value := TIMESTAMP_STRUCT{
					year:     0,
					month:    0,
					day:      0,
					hour:     0,
					minute:   0,
					second:   0,
					fraction: 0,
				}
				if fldlen == 8 {
					workspace = int(binary.LittleEndian.Uint64(fieldDataP[:fldlen]))
				} else if fldlen == 4 {
					workspace = int(binary.LittleEndian.Uint32(fieldDataP[:fldlen]))
				}

				if fldlen == 8 {
					timestamp2struct(workspace, &timestamp_value)
				} else if fldlen == 4 {
					//could not find any case for the same and hence not implemented yet
					//abstime2struct(workspace, &timestamp_value)
				}
				dest[field_lf] = fmt.Sprintf("%02d-%02d-%02d %02d:%02d:%02d.%02d", timestamp_value.year, timestamp_value.month, timestamp_value.day, timestamp_value.hour, timestamp_value.minute, timestamp_value.second, timestamp_value.fraction)
				fldlen = memsize
				elog.Debugf(chopPath(funName()), "field=%d, datatype=TIMESTAMP, value=%s, len=%d ", cur_field+1, dest[field_lf], fldlen)
			}
			break

		case NzTypeNumeric:
			var buffer NumericVar
			var num_parts int
			tupdesc := res.dbosTupleDescriptor
			prec := CTable_i_fieldPrecision(tupdesc, cur_field)
			scale := CTable_i_fieldScale(tupdesc, cur_field)
			count := CTable_i_fieldNumericDigit32Count(tupdesc, cur_field)
			if prec <= 9 {
				num_parts = 1
			} else if prec <= 18 {
				num_parts = 2
			} else {
				num_parts = 4
			}

			var dataBuffer = make([]TNumericDigit, num_parts)

			if NDIGIT_INT64 {
				for i := 0; i < num_parts; i++ {
					dataBuffer[i] = TNumericDigit(TNumericDigit(binary.LittleEndian.Uint64(fieldDataP)))
					fieldDataP.next(8)
				}
			} else {
				for i := 0; i < num_parts; i++ {
					dataBuffer[i] = TNumericDigit(binary.LittleEndian.Uint32(fieldDataP))
					fieldDataP.next(4)
				}
			}

			GOLANG_numeric_load_var(&buffer, dataBuffer, prec, scale, count)
			nValStr := get_str_from_var(&buffer, buffer.rscale)
			dest[field_lf] = nValStr
			fldlen = len(nValStr)
			elog.Debugf(chopPath(funName()), "field=%d, datatype=NzTypeNumeric, value=%s, len=%d ", cur_field+1, dest[field_lf], fldlen)
			break

		case NzTypeBool:
			dest[field_lf] = fieldDataP.byte()
			elog.Debugf(chopPath(funName()), "field=%d, datatype=BOOL, value=%d, len=%d ", cur_field+1, dest[field_lf], fldlen)
		}
		cur_field++
	}

}

func (cn *conn) Sock_clear_socket() {
	p := make([]byte, 100)
	cn.c.Read(p)
}

func (cn *conn) Conn_processAuthResponse() (status bool, err error) {
	flg := false
	res := true
	for flg != true {
		t, _ := cn.recvSingleByte()
		elog.Debugf(chopPath(funName()), "Backend response  %c \n", t)
		if t != 'R' && t != 'N' && t != 'E' {
			cn.recv_n_bytes(8) // do not use this just ignore
		}
		switch t {
		case 'R':
			x, _ := cn.recv_n_bytes(4)
			areq := x.int32()
			elog.Debugf(chopPath(funName()), "Backend response  %d \n", areq)

		case 'K':
			x, _ := cn.recv_n_bytes(8)

			areq := x.int32()
			elog.Debugf(chopPath(funName()), "Backend response PID  %d \n", areq)

			areq = x.int32()
			elog.Debugf(chopPath(funName()), "Backend response KEY  %d \n", areq)

		case 'Z':
			elog.Debugln(chopPath(funName()), "Authentication Successful")
			flg = true
			break

		case 'E':
			responseBuf, _ := cn.recv_n_bytes(2000)
			errorString := responseBuf.string()
			err = errors.New(errorString)
			elog.Fatalf(chopPath(funName()), errorString)
			res = false
			flg = true

		case 'N':
			cn.recv_n_bytes(4) // ignore first 4 bytes
			x, _ := cn.recv_n_bytes(4)
			len := x.int32()
			elog.Debugf(chopPath(funName()), "Backend message(Notice) length %d\n", len)

			responseBuf, _ := cn.recv_n_bytes(len)
			notice := &rows{cn: cn}
			notice.noticetag = responseBuf.string()
			elog.Infof(chopPath(funName()), "Message(Notice) received %s\n", notice.noticetag)

		default:
			err = elog.Fatalf(chopPath(funName()), "Unexpected response: %q", t)
			res = false
		}
	}
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return res, err
	}
	return res, nil
}
func (cn *conn) Conn_authenticate(o values) (status bool, err error) {

	var x readBuf
	t, _ := cn.recvSingleByte() //Expecting 'R'
	if t == 'N' {
		t, _ = cn.recvSingleByte() //Expecting 'R'
	}
	elog.Debugf(chopPath(funName()), "Backend response  %c\n", t)

	if t != 'R' {
		return false, nil
	}

	x, _ = cn.recv_n_bytes(4) //type of password

	res := true
	switch code := x.int32(); code {
	case AUTH_REQ_OK:
		// OK
		//return result as true
		break
	case AUTH_REQ_PASSWORD: //plaintext password
		elog.Debugln(chopPath(funName()), "Password type PLAIN")
		w := cn.writeBuf('p')
		w.string(o["password"])
		elog.Debugf(chopPath(funName()), "Password  %s\n", o["password"])
		err = cn.send(w)
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return false, err
		}

		res, err = cn.Conn_processAuthResponse()
		break

	case AUTH_REQ_MD5: //md5
		elog.Debugln(chopPath(funName()), "Password type MD5")
		salt, _ := cn.recv_n_bytes(2) //salt value
		saltStr := string(salt)
		elog.Debugf(chopPath(funName()), "Salt value  %s\n", saltStr)
		w := cn.writeBuf('p')

		digest := md5.New()
		digest.Write([]byte(saltStr))
		digest.Write([]byte(o["password"]))
		md5Sum := digest.Sum(nil) //md5 sum in byte form (16)
		elog.Debugln(chopPath(funName()), "MD5 sum ", md5Sum)

		sEnc := b64.StdEncoding.EncodeToString(md5Sum) //Base 64 bit encoding (24 bytes)
		sFinal := strings.TrimRight(sEnc, "=")         //remove trailing '=' characters
		elog.Debugln(chopPath(funName()), "Encoded(Base 64bit) ", sFinal)

		w.string(sFinal)
		err = cn.send(w) //send md5 encoded hash
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return false, err
		}

		res, err = cn.Conn_processAuthResponse() //process server response

	case AUTH_REQ_SHA256:
		elog.Debugln(chopPath(funName()), "Password type SHA256")
		salt, _ := cn.recv_n_bytes(2) //salt value
		saltStr := string(salt)
		elog.Debugf(chopPath(funName()), "Salt value  %s\n", saltStr)
		w := cn.writeBuf('p')

		digest := sha256.New()
		digest.Write([]byte(saltStr))
		digest.Write([]byte(o["password"]))
		sha256Sum := digest.Sum(nil)
		elog.Debugln(chopPath(funName()), "sha256 sum ", sha256Sum)

		sEnc := b64.StdEncoding.EncodeToString(sha256Sum) //Base 64 bit encoding (24 bytes)
		sFinal := strings.TrimRight(sEnc, "=")            //remove trailing '=' characters
		elog.Debugln(chopPath(funName()), "Encoded(Base 64bit) ", sFinal)

		w.string(sFinal)
		err = cn.send(w) //send md5 encoded hash
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return false, err
		}
		res, err = cn.Conn_processAuthResponse() //process server response

	default:
		err = elog.Fatalf(chopPath(funName()), "Unknown authentication response: %d", code)
		res = false
	}
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return res, err
	}
	return res, nil
}

func (cn *conn) Conn_send_database(o values) (bool, error) {

	message := HSV2Msg{
		opcode:  HSV2_DB,
		payload: o["dbname"],
	}
	elog.Debugln(chopPath(funName()), "Database name ", message.payload)
	b := cn.writeBuf(0)

	b.int16(message.opcode)
	b.string(message.payload)
	err := cn.send(b)
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return false, err
	}

	beresp, _ := cn.recvSingleByte()
	elog.Debugf(chopPath(funName()), "Backend response %c \n", beresp)
	switch beresp {
	case 'N':
		return true, nil
	case 'E':
		return false, elog.Fatalf(chopPath(funName()), "ERROR_AUTHOR_BAD")
	default:
		return false, elog.Fatalf(chopPath(funName()), "Unknown response: %d", beresp)

	}
	return false, nil
}

/*Cases which will fail:
Client-> Preferred secured; Server-> Only Unsecured
Client-> Preferred Unsecured; Server-> Only Secured
All other cases of client and server combination will be taken care of.
No fall back options for preferred cases.
*/
func (cn *conn) Conn_secure_session() (bool, error) {

	var upgrade func(conn net.Conn) (net.Conn, error)
	var err error
	message := HSV2Msg{
		opcode:  0,
		payload: "",
	}

	information := HSV2_SSL_NEGOTIATE

	for information != 0 {
		b := cn.writeBuf(0)
		switch information {
		case HSV2_SSL_NEGOTIATE:
			/* SecurityLevel meaning
			 * ---------------------------------------
			 *      0	Preferred Unsecured session
			 *      1	Only Unsecured session
			 *      2	Preferred Secured session
			 *      3	Only Secured session
			 */
			message = HSV2Msg{
				opcode:  information,
				payload: cn.opts["securityLevel"],
			}

		case HSV2_SSL_CONNECT:
			message = HSV2Msg{
				opcode: information,
			}
		}
		currSecLevel, _ := strconv.Atoi(message.payload)
		b.int16(message.opcode)
		b.int32(currSecLevel)
		elog.Debugln(chopPath(funName()), "Connection security ", message.opcode, message.payload)

		err = cn.send(b)
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return false, err
		}
		if information == HSV2_SSL_CONNECT {
			cn.c, err = upgrade(cn.c) //It updates connection with SSL
			if err == nil {
				elog.Debugf(chopPath(funName()), "Secured Connect Success")
				information = 0 //if upgrade success come out of the loop

			} else {
				elog.Debugf(chopPath(funName()), err.Error())
			}
		}

		if information != 0 {
			beresp, _ := cn.recvSingleByte()
			elog.Debugf(chopPath(funName()), "Backend response %c ", beresp)
			switch beresp {
			case 'S':
				elog.Debugln(chopPath(funName()), "Attempting Secured session")
				/* The backend sends 'S' only in 3 cases
				 * - Client requests strict SSL and backend supports it.
				 * - Client requests preffered SSL and backend supports it.
				 * - Client requests preffered non-SSL, but backend supports
				 *   only secured sessions.
				 */
				upgrade, err = ssl(cn.opts)
				if err == nil {
					information = HSV2_SSL_CONNECT

				} else {
					elog.Debugf(chopPath(funName()), err.Error())
					/* We failed to initialize SSL_context*/
				}
			case 'N':
				if information == HSV2_SSL_NEGOTIATE {
					elog.Infoln(chopPath(funName()), "Attempting Unsecured session")
				}
				information = 0
			case 'E':
				return false, elog.Fatalf(chopPath(funName()), "ERROR_CONN_FAIL")
			default:
				return false, elog.Fatalf(chopPath(funName()), "Unknown response: %c", beresp)
			}
		}
	}
	return true, nil
}

func (cn *conn) Conn_set_next_dataprotocol() bool {

	switch cn.protocol2 {
	case 0: // Latest-data-protocol to be tried first
		cn.protocol1 = PG_PROTOCOL_3
		cn.protocol2 = PG_PROTOCOL_5
		break

	case PG_PROTOCOL_5:
		cn.protocol1 = PG_PROTOCOL_3
		cn.protocol2 = PG_PROTOCOL_4
		break

	case PG_PROTOCOL_4:
		cn.protocol1 = PG_PROTOCOL_3
		cn.protocol2 = PG_PROTOCOL_3
		break
	}
	elog.Debugln(chopPath(funName()), "Connection protocol set to ", cn.protocol1, cn.protocol2)
	return true
}

func (cn *conn) Conn_send_handshake_version2(o values) (status bool, err error) {

	message := HSV2Msg{
		opcode:  0,
		payload: "",
	}
	information := HSV2_USER
	b := cn.writeBuf(0)

	for information != 0 {
		b = cn.writeBuf(0)
		switch information {
		case HSV2_USER: /* Username */
			message = HSV2Msg{
				opcode:  information,
				payload: o["user"],
			}

			b.int16(message.opcode)
			b.string(message.payload)
			elog.Debugln(chopPath(funName()), "Username ", message.payload)
			information = HSV2_PROTOCOL
			break

		case HSV2_PROTOCOL: /* Postgre data protocol */
			message = HSV2Msg{
				opcode: information,
			}

			b.int16(message.opcode)
			b.int16(cn.protocol1)
			b.int16(cn.protocol2)
			elog.Debugln(chopPath(funName()), "Postgres data protocol ", cn.protocol1, cn.protocol2)
			information = HSV2_REMOTE_PID
			break

		case HSV2_REMOTE_PID: /* Remote PID */
			message = HSV2Msg{
				opcode:  information,
				payload: strconv.Itoa(os.Getpid()),
			}
			b.int16(message.opcode)
			typ, _ := strconv.Atoi(message.payload)
			b.int32(typ)
			elog.Debugln(chopPath(funName()), "Remote PID ", message.payload)
			information = HSV2_CLIENT_TYPE
			break

		case HSV2_CLIENT_TYPE: /* Golang client */

			message = HSV2Msg{
				opcode:  information,
				payload: strconv.Itoa(NPSCLIENT_TYPE_GOLANG), //No Use check below
			}

			b.int16(message.opcode)
			typ, _ := strconv.Atoi(message.payload)
			b.int16(typ)
			elog.Debugln(chopPath(funName()), "Golang client ", message.payload)
			if cn.hsVersion == CP_VERSION_5 {
				information = HSV2_64BIT_VARLENA_ENABLED
			} else {
				information = HSV2_CLIENT_DONE
			}
			break

		case HSV2_64BIT_VARLENA_ENABLED:
			message = HSV2Msg{
				opcode:  information,
				payload: strconv.Itoa(IPS_CLIENT),
			}
			b.int16(message.opcode)
			typ, _ := strconv.Atoi(message.payload)
			b.int16(typ)
			elog.Debugln(chopPath(funName()), "IPS client ", message.payload)
			information = HSV2_CLIENT_DONE
			break

		case HSV2_CLIENT_DONE: /* Finished sending the information */
			message = HSV2Msg{
				opcode: information,
			}

			b = cn.writeBuf(0)
			b.int16(message.opcode)
			b.string(message.payload)
			elog.Debugln(chopPath(funName()), "Finishing sending information")
			information = 0
			break
		}

		err = cn.send(b)
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return false, err
		}
		if information != 0 {
			beresp, _ := cn.recvSingleByte()
			elog.Debugf(chopPath(funName()), "Backend response %c \n", beresp)
			switch beresp {
			case 'N':
				break
			case 'E':
				return false, elog.Fatalf(chopPath(funName()), "ERROR_CONN_FAIL")
			default:
				return false, elog.Fatalf(chopPath(funName()), "Unknown response: %d", beresp)
			}
		}
	}
	return true, nil
}

func (cn *conn) Conn_send_handshake_version4(o values) (status bool, err error) {

	message := HSV2Msg{
		opcode:  0,
		payload: "",
	}
	information := HSV2_USER
	b := cn.writeBuf(0)

	for information != 0 {
		b = cn.writeBuf(0)
		switch information {
		case HSV2_USER: /* Username */
			message = HSV2Msg{
				opcode:  information,
				payload: o["user"],
			}

			b.int16(message.opcode)
			b.string(message.payload)
			elog.Debugln(chopPath(funName()), "Username ", message.payload)
			information = HSV2_APPNAME
			break

		case HSV2_APPNAME: /* App name */
			message = HSV2Msg{
				opcode:  information,
				payload: cn.guardium_applName,
			}
			b.int16(message.opcode)
			b.string(message.payload)
			elog.Debugln(chopPath(funName()), "Appname ", message.payload)
			information = HSV2_CLIENT_OS
			break

		case HSV2_CLIENT_OS: /* OS name */
			message = HSV2Msg{
				opcode:  information,
				payload: cn.guardium_clientOS,
			}
			b.int16(message.opcode)
			b.string(message.payload)
			elog.Debugln(chopPath(funName()), "OS name ", message.payload)
			information = HSV2_CLIENT_HOST_NAME
			break

		case HSV2_CLIENT_HOST_NAME: /* Client Host name */
			message = HSV2Msg{
				opcode:  information,
				payload: cn.guardium_clientHostName,
			}
			b.int16(message.opcode)
			b.string(message.payload)
			elog.Debugln(chopPath(funName()), "Client hostname ", message.payload)
			information = HSV2_CLIENT_OS_USER
			break

		case HSV2_CLIENT_OS_USER: /* client OS User name */
			message = HSV2Msg{
				opcode:  information,
				payload: cn.guardium_clientOSUser,
			}
			b.int16(message.opcode)
			b.string(message.payload)
			elog.Debugln(chopPath(funName()), "ClientOS user ", message.payload)
			information = HSV2_PROTOCOL
			break

		case HSV2_PROTOCOL: /* Postgre data protocol */
			message = HSV2Msg{
				opcode: information,
			}

			b.int16(message.opcode)
			b.int16(cn.protocol1)
			b.int16(cn.protocol2)
			elog.Debugln(chopPath(funName()), "Postgres data protocol ", cn.protocol1, cn.protocol2)
			information = HSV2_REMOTE_PID
			break

		case HSV2_REMOTE_PID: /* Remote PID */
			message = HSV2Msg{
				opcode:  information,
				payload: strconv.Itoa(os.Getpid()),
			}
			b.int16(message.opcode)
			typ, _ := strconv.Atoi(message.payload)
			b.int32(typ)
			elog.Debugln(chopPath(funName()), "Remote PID ", message.payload)
			information = HSV2_CLIENT_TYPE
			break

		case HSV2_CLIENT_TYPE: /* Golang client */

			message = HSV2Msg{
				opcode:  information,
				payload: strconv.Itoa(NPSCLIENT_TYPE_GOLANG), //No Use check below
			}

			b.int16(message.opcode)
			typ, _ := strconv.Atoi(message.payload)
			b.int16(typ)
			elog.Debugln(chopPath(funName()), "Golang client ", message.payload)
			if cn.hsVersion == CP_VERSION_6 {
				information = HSV2_64BIT_VARLENA_ENABLED
			} else {
				information = HSV2_CLIENT_DONE
			}
			break

		case HSV2_64BIT_VARLENA_ENABLED:
			message = HSV2Msg{
				opcode:  information,
				payload: strconv.Itoa(IPS_CLIENT),
			}
			b.int16(message.opcode)
			typ, _ := strconv.Atoi(message.payload)
			b.int16(typ)
			elog.Debugln(chopPath(funName()), "IPS client ", message.payload)
			information = HSV2_CLIENT_DONE
			break

		case HSV2_CLIENT_DONE: /* Finished sending the information */
			message = HSV2Msg{
				opcode: information,
			}

			b = cn.writeBuf(0)
			b.int16(message.opcode)
			b.string(message.payload)
			elog.Debugln(chopPath(funName()), "Finishing sending information")
			information = 0
			break
		}

		err = cn.send(b)
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return false, err
		}
		if information != 0 {
			beresp, _ := cn.recvSingleByte()
			elog.Debugf(chopPath(funName()), "Backend response %c \n", beresp)
			switch beresp {
			case 'N':
				break
			case 'E':
				return false, elog.Fatalf(chopPath(funName()), "ERROR_CONN_FAIL")
			default:
				return false, elog.Fatalf(chopPath(funName()), "Unknown response: %d", beresp)
			}
		}
	}
	return true, nil
}

func (cn *conn) auth(r *readBuf, o values) (err error) {

	switch code := r.int32(); code {
	case 0:
		// OK
	case 3:
		w := cn.writeBuf('p')
		w.string(o["password"])
		err = cn.send(w)
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return err
		}

		t, r, err := cn.recv()
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return err
		}

		if t != 'R' {
			return elog.Fatalf(chopPath(funName()), "unexpected password response: %q", t)
		}

		if r.int32() != 0 {
			return elog.Fatalf(chopPath(funName()), "unexpected authentication response: %q", t)
		}
	case 5:
		s := string(r.next(4))
		w := cn.writeBuf('p')
		w.string("md5" + md5s(md5s(o["password"]+o["user"])+s))
		err = cn.send(w)
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return err
		}
		t, r, err := cn.recv()
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return err
		}

		if t != 'R' {
			return elog.Fatalf(chopPath(funName()), "unexpected password response: %q", t)
		}

		if r.int32() != 0 {
			return elog.Fatalf(chopPath(funName()), "unexpected authentication response: %q", t)
		}
	default:
		return elog.Fatalf(chopPath(funName()), "unknown authentication response: %d", code)
	}
	return nil
}

type format int

const formatText format = 0
const formatBinary format = 1

// One result-column format code with the value 1 (i.e. all binary).
var colFmtDataAllBinary = []byte{0, 1, 0, 1}

// No result-column format codes (i.e. all text).
var colFmtDataAllText = []byte{0, 0}

type stmt struct {
	cn   *conn
	name string
	rowsHeader
	colFmtData []byte
	paramTyps  []oid.Oid
	closed     bool
	query      string
}

func (st *stmt) Close() (err error) {

	if st.closed {
		return nil
	}
	if st.cn.bad {
		return driver.ErrBadConn
	}
	defer st.cn.errRecover(&err)

	st.closed = true

	return nil
}

func (st *stmt) Query(v []driver.Value) (r driver.Rows, err error) {
	if st.cn.bad {
		return nil, driver.ErrBadConn
	}
	defer st.cn.errRecover(&err)
	r, err = st.execQuery(v)
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return r, err
	}
	return r, nil
}

func (st *stmt) Exec(v []driver.Value) (res driver.Result, err error) {
	if st.cn.bad {
		return nil, driver.ErrBadConn
	}
	defer st.cn.errRecover(&err)

	res, _, err = st.exec(v)
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return res, err
	}
	return res, nil
}

func (st *stmt) execQuery(arg []driver.Value) (r driver.Rows, err error) {

	var placeholder string
	placeholder = "?"
	query := st.query
	if len(arg) >= 65536 {
		return nil, elog.Fatalf(chopPath(funName()), "got %d parameters but PostgreSQL only supports 65535 parameters", len(arg))
	}
	if len(arg) != len(st.paramTyps) {
		return nil, elog.Fatalf(chopPath(funName()), "got %d parameters but the statement requires %d", len(arg), len(st.paramTyps))
	}
	for i := 0; i < len(arg); i++ {

		switch arg[i].(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			str := fmt.Sprintf("%d", arg[i])
			query = strings.Replace(query, placeholder, str, 1)
		case []byte:
			str := fmt.Sprintf("X'%s'", arg[i])
			query = strings.Replace(query, placeholder, str, 1)
		case float32, float64:
			str := fmt.Sprintf("%f", arg[i])
			query = strings.Replace(query, placeholder, str, 1)
		case string:
			str := fmt.Sprintf("'%s'", arg[i])
			query = strings.Replace(query, placeholder, str, 1)
		default:
			return nil, elog.Fatalf(chopPath(funName()), "unknown type of parameter")
		}
	}
	return st.cn.simpleQuery(query)
}

func (st *stmt) exec(arg []driver.Value) (res driver.Result, commandTag string, err error) {

	var placeholder string
	placeholder = "?"
	query := st.query
	if len(arg) >= 65536 {
		return nil, placeholder, elog.Fatalf(chopPath(funName()), "got %d parameters but PostgreSQL only supports 65535 parameters", len(arg))
	}
	if len(arg) != len(st.paramTyps) {
		return nil, placeholder, elog.Fatalf(chopPath(funName()), "got %d parameters but the statement requires %d", len(arg), len(st.paramTyps))
	}
	for i := 0; i < len(arg); i++ {

		switch arg[i].(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			str := fmt.Sprintf("%d", arg[i])
			query = strings.Replace(query, placeholder, str, 1)
		case []byte:
			str := fmt.Sprintf("X'%s'", arg[i])
			query = strings.Replace(query, placeholder, str, 1)
		case float32, float64:
			str := fmt.Sprintf("%f", arg[i])
			query = strings.Replace(query, placeholder, str, 1)
		case string:
			str := fmt.Sprintf("'%s'", arg[i])
			query = strings.Replace(query, placeholder, str, 1)
		default:
			return nil, placeholder, elog.Fatalf(chopPath(funName()), "unknown type of parameter")
		}

	}
	return st.cn.simpleExec(query)
}

func (st *stmt) NumInput() int {
	return len(st.paramTyps)
}

// parseComplete parses the "command tag" from a CommandComplete message, and
// returns the number of rows affected (if applicable) and a string
// identifying only the command that was executed, e.g. "ALTER TABLE".  If the
// command tag could not be parsed, parseComplete returns error.
func (cn *conn) parseComplete(commandTag string) (driver.Result, string, error) {
	commandsWithAffectedRows := []string{
		"SELECT ",
		// INSERT is handled below
		"UPDATE ",
		"DELETE ",
		"FETCH ",
		"MOVE ",
		"COPY ",
	}

	var affectedRows *string
	for _, tag := range commandsWithAffectedRows {
		if strings.HasPrefix(commandTag, tag) {
			t := commandTag[len(tag):]
			affectedRows = &t
			commandTag = tag[:len(tag)-1]
			break
		}
	}
	// INSERT also includes the oid of the inserted row in its command tag.
	// Oids in user tables are deprecated, and the oid is only returned when
	// exactly one row is inserted, so it's unlikely to be of value to any
	// real-world application and we can ignore it.
	if affectedRows == nil && strings.HasPrefix(commandTag, "INSERT ") {
		parts := strings.Split(commandTag, " ")
		if len(parts) != 3 {
			cn.bad = true
			return nil, commandTag, elog.Fatalf(chopPath(funName()), "unexpected INSERT command tag %s", commandTag)
		}
		affectedRows = &parts[len(parts)-1]
		commandTag = "INSERT"
	}
	// There should be no affected rows attached to the tag, just return it
	if affectedRows == nil {
		return driver.RowsAffected(0), commandTag, nil
	}
	n, err := strconv.ParseInt(*affectedRows, 10, 64)
	if err != nil {
		cn.bad = true
		return nil, commandTag, elog.Fatalf(chopPath(funName()), "could not parse commandTag: %s", err)
	}
	return driver.RowsAffected(n), commandTag, nil
}

type rowsHeader struct {
	colNames []string
	colTyps  []fieldDesc
	colFmts  []format
}

type rows struct {
	cn     *conn
	finish func()
	rowsHeader
	done      bool
	rb        readBuf
	result    driver.Result
	tag       string
	noticetag string

	next                *rowsHeader
	dbosTuple           bool
	status              int
	dbosTupleDescriptor DbosTupleDesc
}

func (rs *rows) Close() error {
	if finish := rs.finish; finish != nil {
		defer finish()
	}
	// no need to look at cn.bad as Next() will
	for {
		err := rs.Next(nil)
		switch err {
		case nil:
		case io.EOF:
			// rs.Next can return io.EOF on both 'Z' (ready for query) and 'T' (row
			// description, used with HasNextResultSet). We need to fetch messages until
			// we hit a 'Z', which is done by waiting for done to be set.
			if rs.done {
				return nil
			}
		default:
			if err != nil {
				elog.Fatalf(chopPath(funName()), err.Error())
				return err
			}
			return nil
		}
	}
}

func (rs *rows) Columns() []string {
	return rs.colNames
}

func (rs *rows) Result() driver.Result {
	if rs.result == nil {
		return emptyRows
	}
	return rs.result
}

func (rs *rows) Tag() string {
	return rs.tag
}

func (rs *rows) HasNextResultSet() bool {
	hasNext := rs.next != nil && !rs.done
	return hasNext
}

func (rs *rows) NextResultSet() error {
	if rs.next == nil {
		return io.EOF
	}
	rs.rowsHeader = *rs.next
	rs.next = nil
	return nil
}

// QuoteIdentifier quotes an "identifier" (e.g. a table or a column name) to be
// used as part of an SQL statement.  For example:
//
//    tblname := "my_table"
//    data := "my_data"
//    quoted := pq.QuoteIdentifier(tblname)
//    err := db.Exec(fmt.Sprintf("INSERT INTO %s VALUES ($1)", quoted), data)
//
// Any double quotes in name will be escaped.  The quoted identifier will be
// case sensitive when used in a query.  If the input string contains a zero
// byte, the result will be truncated immediately before it.
func QuoteIdentifier(name string) string {
	end := strings.IndexRune(name, 0)
	if end > -1 {
		name = name[:end]
	}
	return `"` + strings.Replace(name, `"`, `""`, -1) + `"`
}

func md5s(s string) string {
	h := md5.New()
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (cn *conn) sendBinaryParameters(b *writeBuf, args []driver.Value) {
	// Do one pass over the parameters to see if we're going to send any of
	// them over in binary.  If we are, create a paramFormats array at the
	// same time.
	var paramFormats []int
	for i, x := range args {
		_, ok := x.([]byte)
		if ok {
			if paramFormats == nil {
				paramFormats = make([]int, len(args))
			}
			paramFormats[i] = 1
		}
	}
	if paramFormats == nil {
		b.int16(0)
	} else {
		b.int16(len(paramFormats))
		for _, x := range paramFormats {
			b.int16(x)
		}
	}

	b.int16(len(args))
	for _, x := range args {
		if x == nil {
			b.int32(-1)
		} else {
			datum := binaryEncode(&cn.parameterStatus, x)
			b.int32(len(datum))
			b.bytes(datum)
		}
	}
}

func (cn *conn) sendBinaryModeQuery(query string, args []driver.Value) error {
	if len(args) >= 65536 {
		return elog.Fatalf(chopPath(funName()), "got %d parameters but PostgreSQL only supports 65535 parameters", len(args))
	}

	b := cn.writeBuf('P')
	b.byte(0) // unnamed statement
	b.string(query)
	b.int16(0)

	b.next('B')
	b.int16(0) // unnamed portal and statement
	cn.sendBinaryParameters(b, args)
	b.bytes(colFmtDataAllText)

	b.next('D')
	b.byte('P')
	b.byte(0) // unnamed portal

	b.next('E')
	b.byte(0)
	b.int32(0)

	b.next('S')
	err := cn.send(b)
	if err != nil {
		elog.Fatalf(chopPath(funName()), err.Error())
		return err
	}

	return nil
}

func (cn *conn) processParameterStatus(r *readBuf) {
	var err error
	param := r.string()
	switch param {
	case "server_version":
		var major1 int
		var major2 int
		var minor int
		_, err = fmt.Sscanf(r.string(), "%d.%d.%d", &major1, &major2, &minor)
		if err == nil {
			cn.parameterStatus.serverVersion = major1*10000 + major2*100 + minor
		}

	case "TimeZone":
		cn.parameterStatus.currentLocation, err = time.LoadLocation(r.string())
		if err != nil {
			cn.parameterStatus.currentLocation = nil
		}

	default:
		// ignore
	}
}

func (cn *conn) processReadyForQuery(r *readBuf) {
	cn.txnStatus = transactionStatus(r.byte())
}

func (cn *conn) readReadyForQuery() error {
	t, r := cn.recv1()
	switch t {
	case 'Z':
		cn.processReadyForQuery(r)
		return nil
	default:
		cn.bad = true
		return elog.Fatalf(chopPath(funName()), "unexpected message %q; expected ReadyForQuery", t)
	}
	return nil
}

func (cn *conn) processBackendKeyData(r *readBuf) {
	cn.processID = r.int32()
	cn.secretKey = r.int32()
}

func (cn *conn) readParseResponse() error {
	t, r := cn.recv1()
	switch t {
	case '1':
		return nil
	case 'E':
		err := parseError(r)
		cn.readReadyForQuery()
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return err
		}
		return nil
	default:
		cn.bad = true
		return elog.Fatalf(chopPath(funName()), "unexpected Parse response %q", t)
	}
}

func (cn *conn) readStatementDescribeResponse() (paramTyps []oid.Oid, colNames []string, colTyps []fieldDesc, err error) {
	for {
		t, r := cn.recv1()
		switch t {
		case 't':
			nparams := r.int16()
			paramTyps = make([]oid.Oid, nparams)
			for i := range paramTyps {
				paramTyps[i] = r.oid()
			}
		case 'n':
			return paramTyps, nil, nil, nil
		case 'T':
			colNames, colTyps = parseStatementRowDescribe(r)
			return paramTyps, colNames, colTyps, nil
		case 'E':
			err := parseError(r)
			cn.readReadyForQuery()
			if err != nil {
				elog.Fatalf(chopPath(funName()), err.Error())
				return nil, nil, nil, err
			}
			return nil, nil, nil, nil
		default:
			cn.bad = true
			return nil, nil, nil, elog.Fatalf(chopPath(funName()), "unexpected Describe statement response %q", t)
		}
	}
}

func (cn *conn) readPortalDescribeResponse() (rowsHeader, error) {
	t, r := cn.recv1()
	switch t {
	case 'T':
		return parsePortalRowDescribe(r), nil
	case 'n':
		return rowsHeader{}, nil
	case 'E':
		err := parseError(r)
		cn.readReadyForQuery()
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return rowsHeader{}, err
		}
		return rowsHeader{}, nil
	default:
		cn.bad = true
		return rowsHeader{}, elog.Fatalf(chopPath(funName()), "unexpected Describe response %q", t)
	}
}

func (cn *conn) readBindResponse() error {
	t, r := cn.recv1()
	switch t {
	case '2':
		return nil
	case 'E':
		err := parseError(r)
		cn.readReadyForQuery()
		if err != nil {
			elog.Fatalf(chopPath(funName()), err.Error())
			return err
		}
		return nil
	default:
		cn.bad = true
		return elog.Fatalf(chopPath(funName()), "unexpected Bind response %q", t)
	}
}

func (cn *conn) postExecuteWorkaround() error {
	// Work around a bug in sql.DB.QueryRow: in Go 1.2 and earlier it ignores
	// any errors from rows.Next, which masks errors that happened during the
	// execution of the query.  To avoid the problem in common cases, we wait
	// here for one more message from the database.  If it's not an error the
	// query will likely succeed (or perhaps has already, if it's a
	// CommandComplete), so we push the message into the conn struct; recv1
	// will return it as the next message for rows.Next or rows.Close.
	// However, if it's an error, we wait until ReadyForQuery and then return
	// the error to our caller.

	for {
		t, r := cn.recv1()
		switch t {
		case 'E':
			err := parseError(r)
			cn.readReadyForQuery()
			if err != nil {
				elog.Fatalf(chopPath(funName()), err.Error())
				return err
			}
			return nil
		case 'C', 'D', 'I':
			// the query didn't fail, but we can't process this message
			err := cn.saveMessage(t, r)
			if err != nil {
				elog.Fatalf(chopPath(funName()), err.Error())
				return err
			}
			return nil
		default:
			cn.bad = true
			return elog.Fatalf(chopPath(funName()), "unexpected message during extended query execution: %q", t)
		}
	}
}

// Only for Exec(), since we ignore the returned data
func (cn *conn) readExecuteResponse(protocolState string) (res driver.Result, commandTag string, err error) {
	for {
		t, r := cn.recv1()
		switch t {
		case 'C':
			if err != nil {
				cn.bad = true
				return nil, "", elog.Fatalf(chopPath(funName()), "unexpected CommandComplete after error %s", err)
			}
			res, commandTag, err = cn.parseComplete(r.string())
		case 'Z':
			cn.processReadyForQuery(r)
			if res == nil && err == nil {
				err = errUnexpectedReady
			}
			if err != nil {
				elog.Fatalf(chopPath(funName()), err.Error())
				return res, commandTag, err
			}
			return res, commandTag, nil
		case 'E':
			err = parseError(r)
		case 'T', 'D', 'I':
			if err != nil {
				cn.bad = true
				return nil, "", elog.Fatalf(chopPath(funName()), "unexpected %q after error %s", t, err)
			}
			if t == 'I' {
				res = emptyRows
			}
			// ignore any results
		default:
			cn.bad = true
			return nil, "", elog.Fatalf(chopPath(funName()), "unknown %s response: %q", protocolState, t)
		}
	}
}

func parseStatementRowDescribe(r *readBuf) (colNames []string, colTyps []fieldDesc) {
	n := r.int16()
	colNames = make([]string, n)
	colTyps = make([]fieldDesc, n)
	for i := range colNames {
		colNames[i] = r.string()
		r.next(6)
		colTyps[i].OID = r.oid()
		colTyps[i].Len = r.int16()
		colTyps[i].Mod = r.int32()
		// format code not known when describing a statement; always 0
		r.next(2)
	}
	return
}

func parsePortalRowDescribe(r *readBuf) rowsHeader {
	n := r.int16()
	colNames := make([]string, n)
	colFmts := make([]format, n)
	colTyps := make([]fieldDesc, n)
	for i := range colNames {
		colNames[i] = r.string()
		//r.next(6)
		colTyps[i].OID = r.oid()
		colTyps[i].Len = r.int16()
		colTyps[i].Mod = r.int32()
		colFmts[i] = format(r.byte())
	}
	return rowsHeader{
		colNames: colNames,
		colFmts:  colFmts,
		colTyps:  colTyps,
	}
}

// parseEnviron tries to mimic some of libpq's environment handling
//
// To ease testing, it does not directly reference os.Environ, but is
// designed to accept its output.
//
// Environment-set connection information is intended to have a higher
// precedence than a library default but lower than any explicitly
// passed information (such as in the URL or connection string).
func parseEnviron(env []string) (out map[string]string) {

	out = make(map[string]string)

	for _, v := range env {
		parts := strings.SplitN(v, "=", 2)

		accrue := func(keyname string) {
			out[keyname] = parts[1]
		}
		unsupported := func() {
			elog.Infof(chopPath(funName()), "setting %v not supported", parts[0])
		}

		// The order of these is the same as is seen in the
		// PostgreSQL 9.1 manual. Unsupported but well-defined
		// keys logged as error; these should be unset prior to
		// execution. Options which pq expects to be set to a
		// certain value are allowed, but must be set to that
		// value if present (they can, of course, be absent).
		switch parts[0] {
		case "PGHOST":
			accrue("host")
		case "PGHOSTADDR":
			unsupported()
		case "PGPORT":
			accrue("port")
		case "PGDATABASE":
			accrue("dbname")
		case "PGUSER":
			accrue("user")
		case "PGPASSWORD":
			accrue("password")
		case "PGSERVICE", "PGSERVICEFILE", "PGREALM":
			unsupported()
		case "PGOPTIONS":
			accrue("options")
		case "PGAPPNAME":
			accrue("application_name")
		case "PGSSLMODE":
			accrue("sslmode")
		case "PGSSLCERT":
			accrue("sslcert")
		case "PGSSLKEY":
			accrue("sslkey")
		case "PGSSLROOTCERT":
			accrue("sslrootcert")
		case "PGREQUIRESSL", "PGSSLCRL":
			unsupported()
		case "PGREQUIREPEER":
			unsupported()
		case "PGKRBSRVNAME", "PGGSSLIB":
			unsupported()
		case "PGCONNECT_TIMEOUT":
			accrue("connect_timeout")
		case "PGCLIENTENCODING":
			accrue("client_encoding")
		case "PGDATESTYLE":
			accrue("datestyle")
		case "PGTZ":
			accrue("timezone")
		case "PGGEQO":
			accrue("geqo")
		case "PGSYSCONFDIR", "PGLOCALEDIR":
			unsupported()
		}
	}
	return out
}

// isUTF8 returns whether name is a fuzzy variation of the string "UTF-8".
func isUTF8(name string) bool {
	// Recognize all sorts of silly things as "UTF-8", like Postgres does
	s := strings.Map(alnumLowerASCII, name)
	return s == "utf8" || s == "unicode"
}

func alnumLowerASCII(ch rune) rune {
	if 'A' <= ch && ch <= 'Z' {
		return ch + ('a' - 'A')
	}
	if 'a' <= ch && ch <= 'z' || '0' <= ch && ch <= '9' {
		return ch
	}
	return -1 // discard
}
