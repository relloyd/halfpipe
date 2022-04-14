home
    launcher
    health
    stop
    transforms

launch
    response json

transforms (list all)
    ID, name, description, status
    links to: status, stats, stop

transform/{}/status
    id, name, status, error

transform/{}/stats
    id, name, stats

transform/{}/stop
    response json

stop (server)
    (only stop if not already stopped; stopped transforms hang around in allTransformInfo for a period of time if above max num resident)
    response json

health
    response json

action
    copy table
    snowflake incr
    ...


at shutdown signal
    call global shutdown
    stop stats dumping
    close channels
        status
        shutdown
    remove self from allTransformInfo


go func for cleanup of allTransformInfo (one in total)
go func for status handling (one per launched transform; closed when transform ends/panics)
