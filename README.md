# Legion Prof Viewer

This repository contains the Legion Prof frontend in Rust. The frontend here is
intended to be used with Legion Prof and is not (typically) used
standalone. Most users want the integrated version (i.e., that can parse Legion
Prof logs and generate a visualization). To use the integrated version of
Legion Prof, clone the [Legion
repository](https://github.com/StanfordLegion/legion) and run:

```
git clone https://github.com/StanfordLegion/legion.git
cargo install --locked --all-features --path legion/tools/legion_prof_rs
```

To start a native viewer right away, run:

```
legion_prof view prof_*.gz
```

To start a server (and attach a viewer to it), run (in separate shells):

```
legion_prof serve prof_*.gz
legion_prof attach http://127.0.0.1:8080/
```

If you really want to run the frontend by itself, continue to the instructions
below.

## Quickstart

### Native

Run:

```
cargo run --release <URL>
```

Ubuntu dependencies:

```
sudo apt-get install libxcb-render0-dev libxcb-shape0-dev libxcb-xfixes0-dev libspeechd-dev libxkbcommon-dev libssl-dev
```

Fedora Rawhide dependencies:

```
dnf install clang clang-devel clang-tools-extra speech-dispatcher-devel libxkbcommon-devel pkg-config openssl-devel libxcb-devel fontconfig-devel
```

### Web Locally

Install dependencies:

```
cargo install --locked trunk
```

Then run:

```
trunk serve
```

Go to <http://127.0.0.1:8080/#dev> in your browser. (The `#dev` skips
client-side caching, so that you don't need to clear your browser cache as you
develop the app.)

### Web Deploy

Install `trunk` as above. Then run:

```
trunk build --release
```

This will generate a static site under `dist` that you can upload. Note that
`trunk` by default assumes the site will live in the root of the domain (e.g.,
`https://example.com/`). If that is not true, add `--public-url ...` to the
`trunk` command where `...` is the path the build is hosted under (e.g.,
`https://example.com/.../`).

### Web Auto-Deploy

This repository is configured via GitHub Actions to deploy automatically on
each push to the `master` branch. You can test it at
<https://legion.stanford.edu/prof-viewer/?url=https://...> where
`https://...` is the URL of the profile to load.
