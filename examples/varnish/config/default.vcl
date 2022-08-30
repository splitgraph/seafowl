vcl 4.1;

backend seafowl {
  .host = "seafowl";
  .port = "8080";
}

# Use a 5s TTL, which means that beyond that, Varnish will revalidate our
# query results with a conditional request. You can also use different
# TTL and Grace values.
sub vcl_backend_response {
  set beresp.ttl = 5s;
  set beresp.grace = 1h;
  if (beresp.status >= 400) {
    set beresp.ttl = 0s;
    set beresp.grace = 0s;
  }
}

# Add some debug headers to see what Varnish is doing
sub vcl_recv {
  unset req.http.x-cache;
}

sub vcl_hit {
    set req.http.x-cache = "hit";
}

sub vcl_miss {
    set req.http.x-cache = "miss";
}

sub vcl_pass {
    set req.http.x-cache = "pass";
}

sub vcl_pipe {
    set req.http.x-cache = "pipe uncacheable";
}

sub vcl_synth {
    set req.http.x-cache = "synth synth";
    set resp.http.x-cache = req.http.x-cache;
}

sub vcl_deliver {
    if (obj.uncacheable) {
        set req.http.x-cache = req.http.x-cache + " uncacheable" ;
    } else {
        set req.http.x-cache = req.http.x-cache + " cached" ;
    }
    set resp.http.x-cache = req.http.x-cache;
}
