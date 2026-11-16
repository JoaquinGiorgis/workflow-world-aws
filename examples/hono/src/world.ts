/**
 * The AWS World of this app, configured from the `WORKFLOW_AWS_*`
 * variables, and made the one the Workflow runtime uses.
 *
 * With `WORKFLOW_TARGET_WORLD=@joaquingiorgis/world-aws` the runtime would
 * create it on its own; creating it here and passing it to `setWorld()`
 * also works when the app is bundled (Lambda), where the runtime cannot
 * load the package by name, and gives this module the World's own methods.
 */
import { createWorld } from '@joaquingiorgis/world-aws';
import { setWorld } from 'workflow/runtime';
import { POST as flow } from '../.well-known/workflow/v1/flow.mjs';

export const world = createWorld();
setWorld(world);

// Deliver queue messages to the flow route's handler in this process,
// instead of POSTing them to /.well-known/workflow/v1/flow.
world.registerHandler('__wkf_workflow_', flow);
