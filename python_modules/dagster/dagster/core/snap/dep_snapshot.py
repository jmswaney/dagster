from collections import namedtuple

from dagster import check
from dagster.core.definitions.container import IContainSolids
from dagster.core.serdes import whitelist_for_serdes


def _upstream_outputs(icontains_solids, solid):
    return icontains_solids.dependency_structure.input_to_upstream_outputs_for_solid(solid.name)


def build_dep_structure_snapshot_from_icontains_solids(icontains_solids):
    check.inst_param(icontains_solids, 'icontains_solids', IContainSolids)
    return DependencyStructureSnapshot(
        solid_invocation_snaps=[
            SolidInvocationSnap(
                solid_name=solid.name,
                solid_def_name=solid.definition.name,
                tags=solid.tags,
                input_dep_snaps=[
                    InputDependencySnap(
                        input_name=input_handle.input_def.name,
                        prev_output_snaps=[
                            OutputHandleSnap(oh.solid.name, oh.output_def.name)
                            for oh in output_handles
                        ],
                    )
                    for input_handle, output_handles in _upstream_outputs(
                        icontains_solids, solid
                    ).items()
                ],
            )
            for solid in icontains_solids.solids
        ]
    )


@whitelist_for_serdes
class DependencyStructureSnapshot(
    namedtuple('_DependencyStructureSnapshot', 'solid_invocation_snaps')
):
    def __new__(cls, solid_invocation_snaps):
        return super(DependencyStructureSnapshot, cls).__new__(
            cls,
            check.list_param(
                solid_invocation_snaps, 'solid_invocation_snaps', of_type=SolidInvocationSnap
            ),
        )


class DependencyStructureSnapshotIndex:
    def __init__(self, dep_structure_snapshot):
        check.inst_param(
            dep_structure_snapshot, 'dep_structure_snapshot', DependencyStructureSnapshot
        )
        self._invocations_dict = {
            si.solid_name: si for si in dep_structure_snapshot.solid_invocation_snaps
        }

    def get_invocation(self, solid_name):
        check.str_param(solid_name, 'solid_name')
        return self._invocations_dict[solid_name]

    def get_upstream_outputs(self, solid_name, input_name):
        check.str_param(solid_name, 'solid_name')
        check.str_param(input_name, 'input_name')

        for input_dep in self.get_invocation(solid_name).input_dep_snaps:
            if input_dep.input_name == input_name:
                return input_dep.prev_output_snaps

        check.failed(
            'Input {input_name} not found for solid {solid_name}'.format(
                input_name=input_name, solid_name=solid_name,
            )
        )

    def get_upstream_output(self, solid_name, input_name):
        outputs = self.get_upstream_outputs(solid_name, input_name)
        check.invariant(len(outputs) == 1)
        return outputs[0]


@whitelist_for_serdes
class OutputHandleSnap(namedtuple('_OutputHandleSnap', 'solid_name output_name')):
    def __new__(cls, solid_name, output_name):
        return super(OutputHandleSnap, cls).__new__(
            cls,
            solid_name=check.str_param(solid_name, 'solid_name'),
            output_name=check.str_param(output_name, 'output_name'),
        )


@whitelist_for_serdes
class InputDependencySnap(namedtuple('_InputDependencySnap', 'input_name prev_output_snaps')):
    def __new__(cls, input_name, prev_output_snaps):
        return super(InputDependencySnap, cls).__new__(
            cls,
            input_name=check.str_param(input_name, 'input_name'),
            prev_output_snaps=check.list_param(
                prev_output_snaps, 'prev_output_snaps', of_type=OutputHandleSnap
            ),
        )


@whitelist_for_serdes
class SolidInvocationSnap(
    namedtuple('_SolidInvocationSnap', 'solid_name solid_def_name tags input_dep_snaps')
):
    def __new__(cls, solid_name, solid_def_name, tags, input_dep_snaps):
        return super(SolidInvocationSnap, cls).__new__(
            cls,
            solid_name=check.str_param(solid_name, 'solid_name'),
            solid_def_name=check.str_param(solid_def_name, 'solid_def_name'),
            tags=check.dict_param(tags, 'tags'),
            input_dep_snaps=check.list_param(
                input_dep_snaps, 'input_dep_snaps', of_type=InputDependencySnap
            ),
        )
