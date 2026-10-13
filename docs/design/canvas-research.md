# Canvas research — pipeline editors

Written before the next canvas implementation pass. Sources are vendor
documentation that ships annotated screenshots (and, where the page offers
it, a Watch link). Image pixels are not reproduced here; the layout claims
below are the captions and region tables on those pages.

This note is about the **transform canvas** inside Transfer Studio. It is
not a second product shell. `studio_canvas` and `studio_schema_browser`
stay default **off**. The wizard step list is unchanged.

## What DataFlow’s canvas is

The recipe is an ordered list of row-local steps. Edges mean order. A cycle
cannot be stored. Post-load SQL (join, aggregate) is not a step; it stays a
plan card that links to SQL models. The engine still runs the list.

That constraint is the filter for every pattern below. A tool that draws an
arbitrary DAG, a runtime status grid, or a warehouse SQL IDE is useful as a
contrast, and a poor thing to copy onto this surface.

Current code the mapping refers to:

- Palette and drop: `apps/web/src/components/transfer/TransferTransformCanvas.tsx`
- Chain model: `apps/web/src/lib/canvasDocument.ts`
- Surface: `apps/web/src/components/transfer/TransformFlow.tsx` (xyflow, loaded only when the flag is on)
- Families: Values, Columns, Row count, Nested JSON (`operationsByFamily` in `apps/web/src/lib/shape.ts`)

## Informatica PowerCenter Designer and IDMC Mapping Designer

Sources:

- [Mapping Designer](https://docs.informatica.com/integration-cloud/data-integration/current-version/mappings/mappings/mapping-designer.html) (IDMC Cloud Data Integration, current version; region table for the annotated screenshot)
- [Configuring the data flow](https://docs.informatica.com/integration-cloud/data-integration/current-version/mappings/mappings/mapping-configuration/configuring-the-data-flow.html)
- [Connecting mapping objects](https://docs.informatica.com/data-integration/powercenter/10-5-7/designer-guide/mappings/connecting-mapping-objects.html) (PowerCenter 10.5.7)
- [Rules and guidelines for connecting mapping objects](https://docs.informatica.com/integration-cloud/cloud-data-integration-for-powercenter/current-version/designer-guide/mappings/connecting-mapping-objects/rules-and-guidelines-for-connecting-mapping-objects.html)
- [Validating a mapping](https://docs.informatica.com/integration-cloud/data-integration/current-version/mappings/mappings/mapping-validation/validating-a-mapping.html)
- [Preview behavior for a mapping](https://docs.informatica.com/integration-cloud/data-integration/current-version/mappings/mappings/data-preview-in-mappings/preview-behavior-for-a-mapping.html)
- July 2026 mappings PDF, same region table: `CDI_July2026_Mappings_en.pdf` on docs.informatica.com

### What it does well

The Mapping Designer screenshot is a full canvas with a fixed frame around it:

1. Properties panel for the selected transformation. It can be resized, or swapped so only the canvas or only the properties show.
2. Header: name, Valid / Invalid, Save, Run (test), undo and redo (until save), and toggles for Inventory, Parameters, Validation, and SQL ELT Query.
3. Transformation palette. Drag a transformation onto the canvas. A new mapping already has a Source and a Target.
4. Canvas.
5. Toolbar: Arrange All, delete, cut/copy/paste, zoom. CLAIRE recommendations are a toggle, not the default chrome.
6. Validation panel lists transformations and their errors. Error icons sit on the objects that are wrong. Refresh after an edit.
7. Bird’s eye view for large mappings and field lineage.

Drop between two already-linked transformations and the designer inserts and connects the new one. Hovering a link also offers Add Transformation. Downstream fields are inherited; they are not retyped by hand.

PowerCenter ports are typed. Sources have only output ports, targets only input ports, transformations a mix of input, output, and input/output. A link is created only when it passes validation. A refused link draws a symbol and does not become an edge. Unconnected ports are allowed and ignored at runtime. Datatypes must be compatible (Char may link to Varchar; identical types are not required). Active and passive transformations do not share a downstream input group. At least one port of an input group must be linked upstream.

Preview runs only when the selected transformation and everything upstream are valid. It shows that transformation and the upstream results. Some transformations cannot be previewed (Data Masking, Sequence, Target, and others). SQL ELT mode hides Run on the canvas and puts the generated query behind a panel toggle.

### Adopt

- The canvas is the large surface. Properties dock beside it and can be collapsed. Undo lives in the header.
- A drop creates an object on the canvas immediately. An unconfigured object stays visible with an error mark. It is not invisible until a form is submitted.
- Insert-between auto-connects. The operator does not draw three links by hand to splice one step.
- Invalid links are refused in the gesture, with a visible reason. They are not stored.
- Validation is a list you can open, not only a toast.
- Preview is blocked when an upstream object is invalid, and the copy says the sample is not the population.
- Generated SQL is a collapsible side panel (SQL ELT Query), never the thing the canvas is made of.
- One icon and a short name per transformation type. Arrange All snaps a messy drag back to a readable flow.

### Avoid

- A port per column. PowerCenter’s port grids are the right model for a mapping with hundreds of fields, and the wrong density for a row recipe whose step already names its column in the inspector.
- Source and Target nodes on this canvas. Those objects already exist as earlier and later wizard steps. Drawing them again duplicates the route.
- Saving an invalid recipe into the engine payload. Informatica can store a mapping whose status is Invalid. DataFlow must not hand a half-specified step to the transfer.
- CLAIRE-style recommendation chrome on the canvas. Suggestions already have their own card.

## Talend Studio (Job designer)

Sources:

- [Talend Studio views](https://help.qlik.com/talend/en-US/discovering-talend-studio/8.0/inside-talend-studio) (8.0, updated 2026-10-09)
- [Adding components to the Job](https://help.qlik.com/talend/en-US/studio-user-guide/8.0-R2024-11/adding-components-to-job)
- [Defining connection settings](https://help.qlik.com/talend/en-US/studio-user-guide/8.0-R2026-05/defining-connection-settings)
- [Viewing in-process data](https://help.qlik.com/talend/en-US/studio-user-guide/8.0-R2026-07/viewing-in-process-data)
- [Displaying Job configuration tabs](https://help.qlik.com/talend/en-US/studio-user-guide/8.0-R2026-04/displaying-job-configuration-tabs-views)

### What it does well

Palette, Designer, and Component view are three different jobs. The palette is searchable by name or by a phrase (“read file row by row”), and the search collapses families that do not match. A drop lands a real component, drawn as its own box, at the point you clicked.

You can also type on the canvas (component creation assistant) and drag the arrow out of an existing component. That second gesture only lists components that can legally connect, and the new component arrives already on a Row Main link.

Connections are typed. Row Main carries the data. Trigger (Run if) carries control and has its own condition. The inspector for a selected link shows the schema on that flow. Data Viewer opens on a component that handles rows, with a row limit, a null filter, and a condition. File components add a Raw Data tab so you can see the file without the schema you configured.

### Adopt

- Search that filters families, not a flat dump.
- Drop position is where the object appears.
- The connect-from-port gesture offers only legal targets. Illegal targets are absent, not drawn and then deleted.
- Distinct link kinds when the semantics differ. For this recipe the split that matters is already in the model: a normal step versus a step that changes row count (filter, divert). Draw those ports differently. Do not invent a second “trigger” edge type the engine cannot run.
- Data Viewer is docked to the step you are inspecting, with an explicit limit, and it is labeled as a sample.

### Avoid

- The Eclipse frame: Repository, Palette, Designer, Component, Run, Problems, Modules, and Scheduler all competing. DataFlow already has a wizard around this step. The canvas does not grow a second IDE.
- Component codes as the primary label (`tMap`, `tFileInputDelimited`). The tile shows the operation summary. The op code can sit on the node as secondary text.
- A palette of thousands of connectors. This palette is the shape catalog’s four families, plus templates.

## SSIS (control flow and data flow)

Sources:

- [SSIS Designer](https://learn.microsoft.com/en-us/sql/integration-services/ssis-designer?view=sql-server-ver17)
- [Data Flow](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/data-flow?view=sql-server-ver17)
- [Integration Services Paths](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/integration-services-paths?view=sql-server-ver17)
- [Data Viewer](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/data-viewer?view=sql-server-ver17)
- [Debugging Data Flow](https://learn.microsoft.com/en-us/sql/integration-services/troubleshooting/debugging-data-flow?view=sql-server-ver17)

### What it does well

SSIS splits control flow from data flow on purpose. Control flow is tasks and precedence constraints (success, failure, completion). Data flow is sources, transformations, and destinations, joined by paths. The toolbox is grouped. You drag an object onto the data-flow surface, then drag a path from an output to the next input. The path is an object: it has metadata, an annotation, and an optional data viewer.

A data viewer attaches to a path, shows a grid buffer by buffer, and pauses the flow until you continue. Detach stops updating the grid without deleting the viewer. Conditional Split can carry a viewer on each output. At design time, a viewer icon sits on the path so you can see where you are looking.

Validation compares the design-time column snapshot with the live schema and posts errors and warnings on the component. Offline mode does not pretend a dead connection was checked.

### Adopt

- One path semantics: output of a step to input of the next. Row-count-changing steps are marked the way SSIS marks an error output as a different path, without adding a second stored edge type.
- The viewer is attached to the flow, shows columns, and states how many rows you are looking at.
- A component with no input yet is visibly unfinished. Configuration of a transform waits until a path has delivered columns. Our inspector already requires a column; the node should show that it is unfinished.
- Do not validate a dead source as if it had been read. The existing refusal when sync mode cannot take a recipe stays a visible disabled canvas, not a fake green chain.

### Avoid

- Two canvases (control flow and data flow) inside the transform step. Precedence constraints belong to schedules and jobs, which already have their own pages.
- Error-output routing as a second graph. Quarantine and `on_error` stay fields on the step. Drawing a red error path would claim a branch the recipe list cannot store.
- Modal editors as the only way to see a path. SSIS hides a lot inside the path editor. The inspector on this canvas stays open beside the selection.

## Apache Airflow UI (3.3)

Source: [UI Overview](https://airflow.apache.org/docs/apache-airflow/stable/ui.html) (Airflow 3.3.2). The page is a screenshot guide. Dark theme is the default in those shots; light theme is shown for comparison. Theme switches from user settings.

### What it does well

Graph view is a dependency picture of a Dag that already exists in code. Nodes are tasks. Edges are dependencies. Click a node and a details panel shows metadata and recent runs. Grid view is the operational surface: tasks by run, color for state, click through to logs. The code tab shows the Dag as parsed. Asset graph is lineage across Dags, not an editor. Command search is on the Dag list.

The layout work in the UI (graph as the wide panel, details beside it, logs as a tab of the details) is the right density for *reading* a pipeline.

### Adopt

- Graph for structure, details panel for the selected object, sample or log in a tab of that panel. Do not navigate away to see the sample.
- Light and dark are both first-class. Status color is not the only way to read a node.
- Command palette (Ctrl+K) for “add this operation” when the rail is long. Already present. Keep it.

### Avoid

- Airflow is not a design canvas. You do not drag operators to invent a Dag. Copying Graph view as the editor would produce a picture of a recipe the operator cannot change by dragging.
- Do not paint design nodes with run state (success, failed, upstream failed). That color language belongs to Jobs. On this canvas, color means selection, disabled, other-table, and invalid configuration.
- Do not make Grid view the transform step. Historical runs are a different page.

## dbt Cloud: Studio IDE lineage and Canvas

Sources:

- [Edit and create dbt models (Canvas)](https://docs.getdbt.com/docs/platform/use-canvas) (Enterprise / Enterprise+)
- [IDE user interface](https://docs.getdbt.com/docs/platform/studio-ide/ide-user-interface)
- [Discover data with Catalog](https://docs.getdbt.com/docs/explore/explore-projects)

### What it does well

Canvas is the closest product to a visual model builder:

1. Drag an operator from the operator toolbar and drop it on the canvas.
2. Click the operator. A configuration panel opens.
3. Each operator has an Output tab (preview of that node) and a SQL Code tab (the SQL the configuration generates).
4. Connect by dragging from a “+” handle to another operator. The line is the flow from source through transforms to the output.
5. You confirm the model by looking at Output as you go.

The IDE lineage tab is a different object: a DAG of models that already exist, two levels upstream and downstream by default, double-click to open the file. Catalog’s Explore Lineage is the full project graph, with lenses and selectors, and it warns that a very large graph may not render. Preview in the IDE runs the SQL in the editor into a results console under the file. CSV upload becomes a source operator plus a model, and the file lands in the warehouse. You cannot invent a source that has never been run in production.

### Adopt

- Operator toolbar of tiles, drop onto the canvas, click to configure.
- Output (sample) and generated SQL are tabs or disclosures on the canvas, with Output first and SQL collapsed.
- A handle you drag from. The connector line appears only when the link is legal.
- Preview is per selected step’s upstream sample, and it is honest about being a sample. Validate remains the population check. That sentence already exists (`samplePopulationBanner`).

### Avoid

- Canvas as a SQL IDE with a picture on top. The Code tab is how you *check* the node, not how you *build* it. Warehouse SQL on this step stays collapsed and links out to SQL models.
- Full-project lineage (Catalog). This canvas is one recipe for the active table. Other tables are dimmed nodes, not a second graph.
- Requiring a production run before a step can be drawn. A file upload in the wizard is a valid source. dbt’s “no source until production” rule does not apply.

## Patterns to adopt (cross-product)

| Pattern | Where it is proven | DataFlow use |
| --- | --- | --- |
| Full-page canvas, inspector docked | IDMC properties panel; Airflow details; dbt configuration panel | Canvas is the transform step body. Inspector stays on the right. |
| Icon tile palette, searchable, grouped | IDMC transformation palette; Talend palette search; dbt operator toolbar; SSIS toolbox groups | Four families, one icon each, summary as the label, filter box kept. |
| Drop creates the object where you drop it | IDMC, Talend, SSIS, dbt Canvas | Drop shows a real node immediately. If required fields are empty, the node is marked invalid and is not written into the recipe until the inspector commits. |
| Insert between two linked steps | IDMC “drop on the link” | Drop whose vertical position falls between two nodes inserts there and the chain reconnects. |
| Drag to reposition, then snap to order | IDMC Arrange All; the recipe is a list | Nodes drag. On drop, order follows vertical position. Layout snaps back to the chain. Positions are not a second source of truth. |
| Ports with refused illegal links | PowerCenter link symbol; Talend “only connectable components”; dbt “+” handle | One input and one output. Output of step *i* may link to input of step *i+1*. Self, backward, and skip links show a reason and create no edge. |
| Row-count path marked differently | PowerCenter active vs passive; SSIS error output | `changesRowCount` keeps the distinct output port. No second stored edge. |
| Undo / redo in the header, not per keystroke | IDMC undo until save; our inspector already commits on blur | Header Undo and Redo stay. A keystroke is not a history entry. |
| Docked sample preview | dbt Output tab; SSIS data viewer; IDMC preview results; Talend Data Viewer | A dock under the stage: sample banner, effect line, Run preview. Collapsed to one row until opened. |
| Warehouse SQL collapsed | IDMC SQL ELT Query toggle; dbt SQL Code tab | Plan cards live in a closed “Warehouse SQL” disclosure. They are not tiles and not nodes. |
| Invalid object stays visible | IDMC error icon on the transformation; Validation panel | Pending node uses the alert icon and the same error the form would have shown. |
| Light and dark from the same tokens | Airflow 3 theme toggle | No new hex colors. `studio-next.css` stays on tokens. |

## Patterns to avoid

- A text list of operations with one shared icon. That reads as a menu, not a palette. It is the current rail.
- A drop that only opens a form. The stage stays empty until Add step. That misses every reference product.
- Storing an illegal edge, or treating “delete edge” as “swap the two steps” without saying so. Reorder is a drag. A bad link is a refusal.
- Port-per-column nodes, source/target nodes, control-flow precedence, Airflow run-state coloring, and a SQL editor as the primary canvas.
- A second preview grid that disagrees with the result pane. The dock summarizes and runs the same preview the step already owns.
- Changing the wizard. Flags default off. Classes and markup for the step list stay put.

## Spec item → product → code

| Spec | Best precedent | Current gap | Design decision |
| --- | --- | --- | --- |
| Full page | IDMC canvas region 4; dbt Canvas | The canvas shared the transform grid with Suggested repairs, so the stage was a sliver beside the operation list. | When the flag is on, the canvas is the full width of the transform step and the stage has an explicit height. Suggested repairs stay above it. The wizard grid is unchanged when the flag is off. The app shell stays. |
| Real drop | IDMC, Talend, SSIS, dbt step 1 | `onDrop` calls `beginAdd`, which only fills the inspector. | Drop places a pending node on the stage at that height. Commit writes the step. Cancel clears the pending node. |
| Drag and reorder | IDMC arrange; list model in `canvasDocument` | `nodesDraggable` is false. Arrow keys call `moveStep`. | Dragging a node reorders by its final vertical index, then `layoutFor` snaps the chain. Keyboard reorder stays. |
| Validated connections | PowerCenter rules; Talend connectable list | Edges are implied by index. Deleting an edge swaps neighbors (`reorderOnDisconnect`). | `classifyChainLink`: neighbor link is the chain; self, backward, and skip return a reason and do not change the list. Disconnect no longer silently swaps. |
| Icon tiles | IDMC palette; dbt toolbar | Every operation used one `layers` icon. | Family icons: Values `edit`, Columns `layers`, Row count `activity`, Nested JSON `code`. Tile and node share the icon; the title sits beside it. |
| Collapsed Warehouse SQL | IDMC SQL ELT toggle; dbt Code tab | “After load” is an open list in the rail. | `<details>` titled Warehouse SQL, closed by default, same plan cards and `#/transforms` link. |
| Docked preview | dbt Output; SSIS viewer; IDMC preview | A status banner above the stage. | Footer dock only, closed by default: sample banner, effect line, Run preview. |
| Undo | IDMC header undo | Header buttons and Ctrl+Z already call the undo stack. | Keep. Pending-node cancel is not an undo entry. Commit is. |

## Out of scope for this pass

- Bird’s eye editing, cut/copy/paste of subgraphs, and field-level lineage. The minimap can stay.
- A per-column port grid.
- Drawing source and target on this canvas.
- Runtime state from the last job.
- Flipping either studio flag on by default.
- Any change to the wizard step list, its Add step control, or global `--df-brand`.

## Sources not used

Unofficial blog screenshots and undated YouTube walkthroughs. Where a doc page offered Watch, the written region table was the source, because the fetch captured the captions and not the video frames.
