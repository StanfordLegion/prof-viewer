# TODO

- [x] Check nested viewport culling
- [x] Slot items by row
- [x] Row check for hover/click
- [x] Better explanatory text
- [x] Utilization plots
- [x] Vertical cursor
- [x] Node selection
- [x] Expand all of a kind (cpu/gpu/etc)Rects on 1-row proc show up at top
- [x] Stop hardcoding kinds
- [x] Multiple profiles
- [x] There is a bug when you move the cursor near the right edge of the screen, the scroll bar gets pushed away
- [x] Timestamps on the vertical cursor
- [x] Horizontal zoom
- [x] Fetch from data source
- [x] Bug in single-row slots not rendered at bottom
- [x] Render data in tiles
- [x] Long-running tasks that cross tile boundary
- [x] Asynchronous data fetch
- [x] Vertical zoom
- [x] Search (with load all data option to get better search results)
- [x] Open window to show key bindings
- [x] Filter by kind
- [x] Task detail view
- [x] Include in the window title the directory where the profile logs are
- [X] Horizontal pan (using keyboard arrow keys)
- [x] Report both field IDs and field names
- [x] On each instance box, add a "zoom to item" button to the "initiator" (i.e. the task that created that instance?)
- [x] When user clicks "zoom to item", e.g. on an instance listed in a task box, also open that new item's popup
- [ ] Horizontal pan (via click and drag, touchpad, or horizontal scroll wheel)
- [ ] Allow horizontal panning past the start/end of the profile, but not so much that none of the actual profile is visible anymore. Out-of-bounds areas should get a different background color.
- [ ] Keyboard bindings (e.g., arrow keys to select panels, space bar to toggle expand/collapse, ESC key to close popups)
- [ ] Editable key bindings?
- [ ] Better error handling (e.g., when the provided URL 404s, there's a permission issue, or parsing fails)
- [ ] Make text in popup boxes copyable
- [ ] Make highlighted boxes clearer

  Currently a highlighted box is shown in red, but that color is already in use in the default color scheme, so highlighted items don't stand out

  Possible solutions:

  - "halo" effect for highlighted box
  - different fill pattern
  - different line color
  - saturate all other boxes except the highlighted one (as happens for search)

- [ ] Have the tooltip box wrap text / scroll vertically if the contents are too long,
      e.g. if we're trying to show full backtraces on provenance, or there's many field names to list
- [ ] Color instances using a heat map based on size
- [ ] The "zoom reset" keyboard shortcut (ctrl + left arrow) doesn't work on MacOS (at least Safari)
- [ ] Thousands separator on large numbers
- [ ] Add average bandwidth measure on copies
- [ ] Add button for "export current view to image"
- [ ] Parse provenance information, according to https://github.com/StanfordLegion/legion/issues/1554
- [ ] In server mode, add a form on the top-level served page, where the user can specify which files to open, instead of having to enter this information manually on the URL as a GET `url=` parameter
- [ ] Vertical scrolling within the "control widgets" group

  Currently if there is not enough vertical space, the "Controls" and "Search" boxes will overlap the footer with the "Show Controls" button, and there is no way to access that, or any controls that are out of view. The footer should always be visible, and anything on top of it should reflow within the available vertical space, with a vertical scrollbar.

- [ ] Combine "Expand by kind" and "Collapse by kind" (if any lines belonging to the clicked group are collapsed then a click expands all; if all are expanded then a click collapses all)
- [ ] Filter channel lines by source/target memory (or memory kind)
- [ ] Support sorting of channels by destination memory
- [ ] Re-number different kinds of processors/memories starting from 0 (e.g. first GPU should be g0 rather than g7)
