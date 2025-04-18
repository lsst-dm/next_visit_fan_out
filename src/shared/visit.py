# This file is part of next_visit_fan_out.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import dataclasses
import typing


@dataclasses.dataclass(frozen=True, kw_only=True)
class NextVisitModelBase:
    """Next Visit Message base without position"""
    salIndex: int
    scriptSalIndex: int
    instrument: str
    groupId: str
    coordinateSystem: int
    startTime: float
    rotationSystem: int
    cameraAngle: float
    filters: str
    dome: int
    duration: float
    nimages: int
    survey: str
    totalCheckpoints: int
    private_sndStamp: float

    def add_detectors(
        self,
        active_detectors: list,
    ) -> list[dict[str, typing.Any]]:
        """Adds and duplicates this message for fanout.

        Parameters
        ----------
        active_detectors: `list`
            The active detectors for an instrument.

        Returns
        -------
        message_list : `list` [`dict`]
            The message list for fan out.
        """
        message = dataclasses.asdict(self)
        message_list: list[dict[str, typing.Any]] = []
        for active_detector in active_detectors:
            temp_message = message.copy()
            temp_message["detector"] = active_detector
            message_list.append(temp_message)
        return message_list


@dataclasses.dataclass(frozen=True, kw_only=True)
class NextVisitModelKeda(NextVisitModelBase):
    position: str  # String because Redis Stream does not support lists.

    @classmethod
    def from_raw_message(cls, message: dict[str, typing.Any]):
        """Factory creating a NextVisitModel from an unpacked message.

        Parameters
        ----------
        message : `dict` [`str`]
            A mapping containing message fields.

        Returns
        -------
        model : `NextVisitModelKeda`
            An object containing the fields in the message.
        """
        # Message may contain fields that aren't in NextVisitModel
        return NextVisitModelKeda(
            salIndex=message["salIndex"],
            scriptSalIndex=message["scriptSalIndex"],
            instrument=message["instrument"],
            groupId=message["groupId"],
            coordinateSystem=message["coordinateSystem"],
            position=str(message["position"]),  # Adds position as string
            startTime=message["startTime"],
            rotationSystem=message["rotationSystem"],
            cameraAngle=message["cameraAngle"],
            filters=message["filters"],
            dome=message["dome"],
            duration=message["duration"],
            nimages=message["nimages"],
            survey=message["survey"],
            totalCheckpoints=message["totalCheckpoints"],
            private_sndStamp=message["private_sndStamp"],
        )


@dataclasses.dataclass(frozen=True, kw_only=True)
class NextVisitModelKnative(NextVisitModelBase):
    position: list[float]

    @classmethod
    def from_raw_message(cls, message: dict[str, typing.Any]):
        """Factory creating a NextVisitModel from an unpacked message.

        Parameters
        ----------
        message : `dict` [`str`]
            A mapping containing message fields.

        Returns
        -------
        model : `NextVisitModelKnative`
            An object containing the fields in the message.
        """
        return NextVisitModelKnative(
            salIndex=message["salIndex"],
            scriptSalIndex=message["scriptSalIndex"],
            instrument=message["instrument"],
            groupId=message["groupId"],
            coordinateSystem=message["coordinateSystem"],
            position=message["position"],
            startTime=message["startTime"],
            rotationSystem=message["rotationSystem"],
            cameraAngle=message["cameraAngle"],
            filters=message["filters"],
            dome=message["dome"],
            duration=message["duration"],
            nimages=message["nimages"],
            survey=message["survey"],
            totalCheckpoints=message["totalCheckpoints"],
            private_sndStamp=message["private_sndStamp"],
        )
